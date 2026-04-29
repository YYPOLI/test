import ast
import pandas as pd


class HexParser:
    """Utility class for decoding on-chain raw data (permit inputs, transferFrom inputs, trace addresses)."""

    UINT256_MAX = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
    INFINITE_THRESHOLD = UINT256_MAX
    LONG_DEADLINE_THRESHOLD = UINT256_MAX

    @staticmethod
    def safe_parse_trace(trace_raw):
        """Parse trace_address from BigQuery export formats: '[0, 1]' or '0,1'."""
        if pd.isna(trace_raw) or str(trace_raw).strip() == "":
            return []
        trace_str = str(trace_raw)
        try:
            if trace_str.strip().startswith('['):
                return ast.literal_eval(trace_str)
            return [int(x) for x in trace_str.split(',') if x.strip().isdigit()]
        except Exception:
            return []

    @staticmethod
    def parse_permit(input_hex):
        """
        Decode EIP-2612 permit input data.
        Layout: Selector(4) + Owner(32) + Spender(32) + Value(32) + Deadline(32)
        """
        try:
            if not isinstance(input_hex, str) or len(input_hex) < 202:
                return None
            h = input_hex[2:] if input_hex.startswith('0x') else input_hex
            return {
                'p_owner': '0x' + h[8 + 24:8 + 64],
                'p_spender': '0x' + h[72 + 24:72 + 64],
                'p_value': int(h[136:200], 16),
                'p_deadline': int(h[200:264], 16),
            }
        except Exception:
            return None

    @staticmethod
    def parse_transfer(input_hex):
        """
        Decode ERC-20 transferFrom input data.
        Layout: Selector(4) + From(32) + To(32) + Value(32)
        """
        try:
            if pd.isna(input_hex) or len(str(input_hex)) < 10:
                return None
            h = input_hex[2:] if input_hex.startswith('0x') else input_hex
            return {
                'tf_from': '0x' + h[8 + 24:8 + 64],
                'tf_to': '0x' + h[72 + 24:72 + 64],
                'tf_value': int(h[136:200], 16),
            }
        except Exception:
            return None
