from unittest.mock import Mock

from jobs.tasks import _consolidate_marketsegmenter_ai_results


def test_consolidation_forces_out_of_scope_when_below_min(tmp_path):
    source = tmp_path / "ai.csv"
    source.write_text(
        '"google_place_id";"ai_segment_source";"ai_selected_for_review";"ai_segment_suggested";"fyre_market_segment_type0";"fyre_market_segment_type1";"fyre_market_segment_type2";"fyre_market_segment_type3";"segmentation_confidence"\n'
        '"abc";"rules_below_min_threshold";"no";"hors cible";"restaurant";"pizza";"";"";"0.10"\n',
        encoding="utf-8-sig",
    )
    final_csv = tmp_path / "final.csv"
    mock_bq = Mock()
    mock_bq.write_segmented_rows_iterable.side_effect = lambda table_name, rows, batch_size=None: len(list(rows))

    metrics = _consolidate_marketsegmenter_ai_results(
        ai_csv_path=source,
        final_csv_path=final_csv,
        output_table_name="out_table",
        process_id="pid",
        low_conf_threshold=0.65,
        min_conf_threshold=0.20,
        progress=lambda *_: None,
        log=lambda *_: None,
        bq=mock_bq,
        write_batch_size=100,
    )

    assert metrics["consolidated_out_of_scope"] == 1
    assert metrics["rows_written"] == 1
    assert "hors cible" in final_csv.read_text(encoding="utf-8-sig")
