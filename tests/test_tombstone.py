"""Tests for TombstoneStore: soft-delete with optimistic locking."""

import json
import tempfile
import shutil
from pathlib import Path

import pytest

from flatseek.core.tombstone import TombstoneStore, ConcurrentModificationError


class TestTombstoneStore:
    """Tests for TombstoneStore basic operations."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tombstone_test_"))
        self.store = TombstoneStore(str(self.tmp))

    def teardown_method(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_mark_deleted_single(self):
        """Mark a single doc_id as deleted."""
        count, conflicts = self.store.mark_deleted([42])
        assert count == 1
        assert conflicts == []
        assert self.store.is_deleted(42)

    def test_mark_deleted_multiple(self):
        """Mark multiple doc_ids as deleted."""
        count, conflicts = self.store.mark_deleted([1, 2, 3])
        assert count == 3
        assert conflicts == []
        assert self.store.is_deleted(1)
        assert self.store.is_deleted(2)
        assert self.store.is_deleted(3)

    def test_mark_deleted_idempotent(self):
        """Marking same doc_id twice returns 0 new deletions."""
        self.store.mark_deleted([5])
        count, conflicts = self.store.mark_deleted([5])
        assert count == 0
        assert conflicts == []
        assert self.store.is_deleted(5)

    def test_filter_alive(self):
        """filter_alive returns only non-deleted doc_ids."""
        self.store.mark_deleted([2, 5])
        alive = self.store.filter_alive([1, 2, 3, 4, 5, 6])
        assert alive == [1, 3, 4, 6]

    def test_filter_alive_empty(self):
        """filter_alive with no deletions returns all."""
        alive = self.store.filter_alive([1, 2, 3])
        assert alive == [1, 2, 3]

    def test_get_deleted_count(self):
        """get_deleted_count returns correct count."""
        assert self.store.get_deleted_count() == 0
        self.store.mark_deleted([1, 2, 3])
        assert self.store.get_deleted_count() == 3
        self.store.mark_deleted([1])  # already deleted
        assert self.store.get_deleted_count() == 3

    def test_replay_wal_rebuilds_state(self):
        """WAL replay restores deleted state after new store instance."""
        self.store.mark_deleted([10, 20, 30])

        # Create new store instance - should replay WAL
        store2 = TombstoneStore(str(self.tmp))
        assert store2.is_deleted(10)
        assert store2.is_deleted(20)
        assert store2.is_deleted(30)
        assert not store2.is_deleted(5)
        store2.close()


class TestOptimisticLocking:
    """Tests for optimistic locking / concurrent modification detection."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tombstone_locking_test_"))
        self.store = TombstoneStore(str(self.tmp))

    def teardown_method(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_version_increments_on_delete(self):
        """Version increments each time a doc is deleted."""
        v0 = self.store.get_version(1)
        self.store.mark_deleted([1])
        v1 = self.store.get_version(1)
        assert v1 > v0

    def test_optimistic_lock_conflict(self):
        """Delete with wrong expected version returns conflict."""
        self.store.mark_deleted([7])  # version becomes 1
        v7 = self.store.get_version(7)

        # Try to delete with old version - should conflict
        count, conflicts = self.store.mark_deleted(
            [7], expected_versions={7: v7 - 1}
        )
        assert count == 0
        assert conflicts == [(7, v7)]

    def test_optimistic_lock_success(self):
        """Delete with correct expected version succeeds."""
        self.store.mark_deleted([8])  # version becomes 1
        v8 = self.store.get_version(8)

        count, conflicts = self.store.mark_deleted(
            [8], expected_versions={8: v8}
        )
        assert count == 0  # already deleted
        assert conflicts == []

    def test_optimistic_lock_mixed(self):
        """Mixed scenario: one conflict, one success."""
        self.store.mark_deleted([9])  # version 1
        v9 = self.store.get_version(9)

        # 9 conflicts (old version), 10 succeeds (never deleted)
        count, conflicts = self.store.mark_deleted(
            [9, 10], expected_versions={9: v9 - 1, 10: 0}
        )
        assert count == 1  # only 10
        assert conflicts == [(9, v9)]

    def test_version_persists_across_restart(self):
        """Version is restored after store restart."""
        self.store.mark_deleted([99])
        v99 = self.store.get_version(99)

        store2 = TombstoneStore(str(self.tmp))
        assert store2.get_version(99) == v99
        store2.close()


class TestIdFieldMapping:
    """Tests for natural key → doc_id mapping."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tombstone_idmap_test_"))
        self.store = TombstoneStore(str(self.tmp))

    def teardown_method(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_set_and_get_doc_id(self):
        """set_doc_id_for_field + get_doc_id_for_field round-trip."""
        self.store.set_doc_id_for_field("email", "alice@example.com", 42)
        doc_id = self.store.get_doc_id_for_field("email", "alice@example.com")
        assert doc_id == 42

    def test_get_doc_id_not_found(self):
        """get_doc_id_for_field returns None for missing key."""
        doc_id = self.store.get_doc_id_for_field("email", "nobody@example.com")
        assert doc_id is None

    def test_remove_doc_id(self):
        """remove_doc_id_for_field removes mapping."""
        self.store.set_doc_id_for_field("email", "bob@example.com", 55)
        self.store.remove_doc_id_for_field("email", "bob@example.com")
        doc_id = self.store.get_doc_id_for_field("email", "bob@example.com")
        assert doc_id is None

    def test_multiple_fields(self):
        """Multiple natural key fields coexist."""
        self.store.set_doc_id_for_field("email", "carol@example.com", 10)
        self.store.set_doc_id_for_field("phone", "+1234567890", 20)

        assert self.store.get_doc_id_for_field("email", "carol@example.com") == 10
        assert self.store.get_doc_id_for_field("phone", "+1234567890") == 20


class TestBitmapGrowth:
    """Tests for bitmap dynamic growth."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tombstone_growth_test_"))
        self.store = TombstoneStore(str(self.tmp))

    def teardown_method(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_grow_for_large_doc_id(self):
        """Bitmap grows to accommodate large doc_ids."""
        # Delete a doc with very large ID
        count, conflicts = self.store.mark_deleted([1_000_000])
        assert count == 1
        assert self.store.is_deleted(1_000_000)

    def test_is_deleted_out_of_range_returns_false(self):
        """is_deleted returns False for doc_id beyond current bitmap."""
        assert not self.store.is_deleted(999)
        assert not self.store.is_deleted(9999)
