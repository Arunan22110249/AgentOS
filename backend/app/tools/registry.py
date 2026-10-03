"""Tool registry and management."""
from typing import Any, Callable, Dict, Optional

from app.database.models import Tool
from app.observability.logging import get_logger
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


class ToolInput(BaseModel):
    """Tool input schema."""

    type: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    required: list[str] = Field(default_factory=list)


class ToolDefinition(BaseModel):
    """Tool definition."""

    name: str
    description: str
    input_schema: ToolInput
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 30


class BuiltInTools:
    """Built-in tools for demonstration."""

    @staticmethod
    def http_request(
        url: str,
        method: str = "GET",
        headers: Optional[Dict] = None,
        body: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request."""
        import asyncio
        from urllib.parse import urlparse

        import httpx

        # SSRF protection: block requests to localhost, private IPs, and cloud metadata endpoints
        try:
            parsed_url = urlparse(url)
            hostname = parsed_url.hostname or ""

            # Block localhost and private IPs
            blocked_hosts = [
                "localhost",
                "127.0.0.1",
                "0.0.0.0",
                "::1",
                "169.254.169.254",
            ]
            if hostname.lower() in blocked_hosts:
                return {
                    "error": "SSRF protection: localhost and metadata endpoints are blocked"
                }

            # Block private IP ranges
            if hostname.startswith(
                (
                    "10.",
                    "172.16.",
                    "172.17.",
                    "172.18.",
                    "172.19.",
                    "172.20.",
                    "172.21.",
                    "172.22.",
                    "172.23.",
                    "172.24.",
                    "172.25.",
                    "172.26.",
                    "172.27.",
                    "172.28.",
                    "172.29.",
                    "172.30.",
                    "172.31.",
                    "192.168.",
                )
            ):
                return {"error": "SSRF protection: private IP ranges are blocked"}
        except Exception as e:
            return {"error": f"URL validation error: {str(e)}"}

        async def _request():
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method, url, headers=headers, content=body
                )
                return {
                    "status_code": response.status_code,
                    "headers": dict(response.headers),
                    "body": response.text,
                }

        try:
            return asyncio.run(_request())
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def calculator(expression: str) -> Dict[str, Any]:
        """Simple calculator tool."""
        try:
            # Simple safe evaluation
            result = eval(expression, {"__builtins__": {}}, {})
            return {"result": result}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def json_transform(data: Dict[str, Any], transformation: str) -> Dict[str, Any]:
        """Transform JSON data."""
        try:
            # Simple transformation logic
            if transformation == "stringify":
                import json

                return {"result": json.dumps(data)}
            elif transformation == "keys":
                return {"result": list(data.keys())}
            elif transformation == "values":
                return {"result": list(data.values())}
            else:
                return {"error": f"Unknown transformation: {transformation}"}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def text_extract(text: str, pattern: str) -> Dict[str, Any]:
        """Extract text using regex."""
        import re

        try:
            matches = re.findall(pattern, text)
            return {"matches": matches}
        except Exception as e:
            return {"error": str(e)}


class ToolRegistry:
    """Registry for tools."""

    def __init__(self):
        self.tools: Dict[str, Dict[str, Any]] = {}
        self._load_builtin_tools()

    def _load_builtin_tools(self):
        """Load built-in tools."""
        builtin = [
            {
                "name": "http_request",
                "description": "Make HTTP requests to external APIs",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "URL to request"},
                        "method": {
                            "type": "string",
                            "enum": ["GET", "POST", "PUT", "DELETE"],
                            "default": "GET",
                        },
                        "headers": {"type": "object", "description": "HTTP headers"},
                        "body": {"type": "string", "description": "Request body"},
                    },
                    "required": ["url"],
                },
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "status_code": {"type": "integer"},
                        "headers": {"type": "object"},
                        "body": {"type": "string"},
                    },
                },
                "timeout_seconds": 30,
            },
            {
                "name": "calculator",
                "description": "Perform mathematical calculations",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "expression": {
                            "type": "string",
                            "description": "Math expression to evaluate",
                        },
                    },
                    "required": ["expression"],
                },
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "result": {"type": "number"},
                    },
                },
                "timeout_seconds": 5,
            },
            {
                "name": "json_transform",
                "description": "Transform JSON data",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "data": {"type": "object", "description": "Data to transform"},
                        "transformation": {
                            "type": "string",
                            "enum": ["stringify", "keys", "values"],
                            "description": "Transformation type",
                        },
                    },
                    "required": ["data", "transformation"],
                },
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "result": {"type": "string"},
                    },
                },
                "timeout_seconds": 5,
            },
            {
                "name": "text_extract",
                "description": "Extract text using regex patterns",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "text": {
                            "type": "string",
                            "description": "Text to extract from",
                        },
                        "pattern": {"type": "string", "description": "Regex pattern"},
                    },
                    "required": ["text", "pattern"],
                },
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "matches": {"type": "array", "items": {"type": "string"}},
                    },
                },
                "timeout_seconds": 5,
            },
        ]

        for tool in builtin:
            self.tools[tool["name"]] = tool

    def register_tool(
        self, name: str, definition: Dict[str, Any], handler: Optional[Callable] = None
    ):
        """Register a tool."""
        self.tools[name] = definition
        if handler:
            self.tools[name]["_handler"] = handler

    def get_tool(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a tool by name."""
        return self.tools.get(name)

    def list_tools(self) -> list[Dict[str, Any]]:
        """List all available tools."""
        return [
            {k: v for k, v in tool.items() if k != "_handler"}
            for tool in self.tools.values()
        ]

    async def execute_tool(
        self,
        name: str,
        inputs: Dict[str, Any],
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Execute a tool."""
        import asyncio

        tool = self.get_tool(name)
        if not tool:
            logger.error(f"Tool not found: {name}")
            return {"error": f"Tool not found: {name}"}

        # Get handler
        handler = tool.get("_handler")
        if handler is None:
            # Try built-in
            handler = getattr(BuiltInTools, name, None)

        if handler is None:
            logger.error(f"No handler for tool: {name}")
            return {"error": f"No handler for tool: {name}"}

        # Use tool timeout if not specified
        timeout = timeout_seconds or tool.get("timeout_seconds", 30)

        try:
            if asyncio.iscoroutinefunction(handler):
                result = await asyncio.wait_for(handler(**inputs), timeout=timeout)
            else:
                result = await asyncio.to_thread(handler, **inputs, timeout=timeout)

            logger.info(f"Tool executed successfully: {name}")
            return result
        except asyncio.TimeoutError:
            logger.error(f"Tool execution timeout: {name}")
            return {"error": f"Tool execution timed out after {timeout} seconds"}
        except Exception as e:
            logger.error(f"Tool execution error: {name}: {str(e)}")
            return {"error": str(e)}


async def load_tenant_tools(
    session: AsyncSession, tenant_id: str
) -> Dict[str, Dict[str, Any]]:
    """Load tenant-specific tools from database."""
    try:
        result = await session.execute(
            select(Tool).where(
                Tool.tenant_id == tenant_id,
                Tool.is_active == True,
            )
        )
        tools = result.scalars().all()

        tool_dict = {}
        for tool in tools:
            tool_dict[tool.name] = {
                "id": tool.id,
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
                "output_schema": tool.output_schema,
                "timeout_seconds": tool.execution_timeout_seconds,
            }

        return tool_dict
    except Exception as e:
        logger.error(f"Error loading tenant tools: {str(e)}")
        return {}


# Global tool registry
_registry = ToolRegistry()


def get_tool_registry() -> ToolRegistry:
    """Get the global tool registry."""
    return _registry
