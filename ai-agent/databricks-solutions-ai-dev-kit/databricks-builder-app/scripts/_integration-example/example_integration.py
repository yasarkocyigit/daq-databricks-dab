#!/usr/bin/env python3
"""
Minimal example of embedding ai-dev-kit into a custom app.

This runs the same Claude Agent SDK-based agent used by databricks-builder-app,
with Databricks MCP tools and skills loaded. The agent can:
- Execute SQL queries via warehouses
- Run Python/PySpark on clusters
- Manage Unity Catalog objects
- Create and run jobs/pipelines
- Read/write files

Run with: python example_integration.py
"""

import asyncio
import os
import sys
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import the same agent service used by databricks-builder-app
# This uses claude-agent-sdk with Databricks MCP tools loaded in-process
from server.services.agent import stream_agent_response
from databricks_tools_core import set_databricks_auth, clear_databricks_auth


async def run_agent(message: str, project_id: str = "demo") -> None:
    """Run the Claude agent with Databricks tools and stream output."""
    
    # Get credentials from environment
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        print("Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env")
        sys.exit(1)
    
    print(f"Databricks workspace: {host}")
    print(f"Message: {message}")
    print("Running Claude agent with Databricks MCP tools...")
    print("-" * 50)
    
    # Set auth context for this request
    set_databricks_auth(host, token)
    
    try:
        # Create project directory if needed
        project_dir = Path("projects") / project_id
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Stream agent response
        async for event in stream_agent_response(
            project_id=project_id,
            message=message,
            databricks_host=host,
            databricks_token=token,
        ):
            event_type = event.get("type")
            
            # Handle different event types
            if event_type == "text_delta":
                # Token-by-token streaming text
                print(event["text"], end="", flush=True)
                
            elif event_type == "text":
                # Complete text block (if streaming disabled)
                print(event["text"])
                
            elif event_type == "thinking_delta":
                # Token-by-token thinking (extended thinking mode)
                pass  # Usually hidden from users
                
            elif event_type == "tool_use":
                # Agent is invoking a tool
                print(f"\n[Using tool: {event['tool_name']}]", flush=True)
                
            elif event_type == "tool_result":
                # Tool execution completed
                content = event.get("content", "")
                is_error = event.get("is_error", False)
                if is_error:
                    print(f"\n[Tool error: {content[:200]}]", flush=True)
                else:
                    # Truncate long results for display
                    preview = content[:200] + "..." if len(content) > 200 else content
                    print(f"\n[Tool result: {preview}]", flush=True)
                    
            elif event_type == "result":
                # Session completed
                print("\n" + "-" * 50)
                print(f"Session ID: {event.get('session_id')}")
                print(f"Duration: {event.get('duration_ms')}ms")
                if event.get("total_cost_usd"):
                    print(f"Cost: ${event.get('total_cost_usd'):.4f}")
                    
            elif event_type == "error":
                # Error occurred
                print(f"\n[ERROR: {event.get('error')}]", file=sys.stderr)
                
            elif event_type == "keepalive":
                # Long operation keepalive
                pass  # Usually hidden
                
    finally:
        # Always clear auth context
        clear_databricks_auth()


async def main():
    """Main entry point."""
    # Default message - can be customized
    message = "List my available SQL warehouses and tell me which one is running."
    
    # Override with command line argument if provided
    if len(sys.argv) > 1:
        message = " ".join(sys.argv[1:])
    
    await run_agent(message)


if __name__ == "__main__":
    asyncio.run(main())
