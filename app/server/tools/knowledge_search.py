"""Knowledge base search tool for Q&A agent."""

import os
from ..config import get_workspace_client


async def search_knowledge_base(query: str) -> list[dict]:
    """Search the FAQ/documentation knowledge base using Vector Search.

    Returns relevant FAQ entries and documentation passages.
    """
    client = get_workspace_client()
    index_name = os.environ.get(
        "VS_INDEX_KNOWLEDGE",
        "enbridge_operations.uc_advisor.knowledge_vs_index",
    )

    try:
        response = client.vector_search_indexes.query_index(
            index_name=index_name,
            columns=["id", "question", "answer", "category", "source"],
            query_text=query,
            num_results=5,
        )

        results = []
        if response.result and response.result.data_array:
            columns = [c.name for c in response.result.manifest.columns]
            for row in response.result.data_array:
                entry = dict(zip(columns, row))
                results.append(entry)
        return results
    except Exception as e:
        return [{"error": f"Knowledge base search failed: {str(e)}"}]
