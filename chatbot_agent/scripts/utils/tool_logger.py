# this script helps with logging metadata about the tools used

from datetime import datetime

tool_usage_log = []

def log_tool_usage(tool_name: str, metadata: dict):
    tool_usage_log.append({
        "tool_name": tool_name,
        "timestamp": datetime.now().isoformat(),
        "metadata": metadata
    })

def clear_tool_log():
    tool_usage_log.clear()

def get_tool_log():
    return tool_usage_log.copy()
