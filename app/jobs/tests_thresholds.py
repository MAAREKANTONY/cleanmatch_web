from pathlib import Path

from jobs.tasks import _consolidate_marketsegmenter_ai_results


def test_consolidation_forces_out_of_scope_when_below_min(tmp_path):
    source = tmp_path / 'ai.csv'
    source.write_text(
        '"google_place_id";"ai_segment_source";"ai_selected_for_review";"ai_segment_suggested";"fyre_market_segment_type0";"fyre_market_segment_type1";"fyre_market_segment_type2";"fyre_market_segment_type3";"segmentation_confidence"\n'
        '"abc";"rules_below_min_threshold";"no";"hors cible";"restaurant";"pizza";"";"";"0.10"\n',
        encoding='utf-8-sig',
    )
    final_csv = tmp_path / 'final.csv'

    rows, bq_rows = _consolidate_marketsegmenter_ai_results(source, final_csv, 'pid', 0.65, 0.20, lambda *_: None, lambda *_: None)

    assert rows[0]['market_segment_type0'] == 'hors cible'
    assert rows[0]['market_segment_type1'] == ''
    assert bq_rows[0]['market_segment_type0'] == 'hors cible'
