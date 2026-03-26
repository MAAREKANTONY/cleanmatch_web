from unittest.mock import Mock

from jobs.tasks import _build_bigquery_selected_columns, _consolidate_marketsegmenter_ai_results


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


def test_build_bigquery_selected_columns_dedupes_mapping_values():
    columns = _build_bigquery_selected_columns({
        "marketsegmenter_mapping": {"name": "name", "address": "address"},
        "ai_review_mapping": {"name": "name", "website": "website"},
    })

    assert columns[:3] == ["name", "address", "website"]
    assert "google_place_id" in columns
    assert len(columns) == len(set(columns))


def test_cleanup_empty_dir_removes_empty_directory(tmp_path):
    from jobs.tasks import _cleanup_empty_dir

    removed = []
    workdir = tmp_path / 'jobdir'
    workdir.mkdir()

    _cleanup_empty_dir(workdir, log=lambda message: removed.append(message), tracker=None)

    assert not workdir.exists()
    assert any('Répertoire de travail supprimé' in message for message in removed)
