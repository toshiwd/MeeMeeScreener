from __future__ import annotations

from external_analysis.contracts.analysis_output import AnalysisOutputContract, analysis_output_from_result


def assemble_tradex_analysis_output(*, result_payload: dict[str, object]) -> AnalysisOutputContract:
    return analysis_output_from_result(result=result_payload)
