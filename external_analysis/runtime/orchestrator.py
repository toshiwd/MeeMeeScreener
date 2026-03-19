from __future__ import annotations

from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import AnalysisOutputContract
from .analysis_adapter import build_tradex_analysis_payload
from .input_normalization import normalize_tradex_analysis_input
from .output_assembler import assemble_tradex_analysis_output


def run_tradex_analysis(input_contract: AnalysisInputContract) -> AnalysisOutputContract:
    normalized_input = normalize_tradex_analysis_input(input_contract)
    result_payload = build_tradex_analysis_payload(normalized_input)
    return assemble_tradex_analysis_output(result_payload=result_payload)
