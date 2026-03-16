"""Foundation Model API client utilities.

The tool-calling loop has moved to agents/base.py.
This module is kept for backward compatibility of get_llm_client().
"""

from .agents.base import get_llm_client, _message_to_dict

__all__ = ["get_llm_client", "_message_to_dict"]
