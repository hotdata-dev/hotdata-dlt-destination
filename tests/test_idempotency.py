from hotdata_dlt_destination.idempotency import compute_batch_key, compute_row_key


def test_batch_key_is_stable() -> None:
    items = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
    first = compute_batch_key("orders", items)
    second = compute_batch_key("orders", items)
    assert first == second


def test_row_key_changes_when_payload_changes() -> None:
    left = compute_row_key("orders", {"id": 1, "value": "a"})
    right = compute_row_key("orders", {"id": 1, "value": "b"})
    assert left != right
