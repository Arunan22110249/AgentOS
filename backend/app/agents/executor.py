"""Agent execution system."""
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import asyncio
from pydantic import BaseModel
from app.tools.registry import get_tool_registry
from app.observability.logging import get_logger, Metrics


logger = get_logger(__name__)


class AgentMessage(BaseModel):
    """Agent message."""
    role: str  # "user", "assistant"
    content: str


class AgentState(BaseModel):
    """Agent execution state."""
    agent_id: str
    conversation_history: List[AgentMessage] = []
    memory: Dict[str, Any] = {}
    iteration_count: int = 0
    total_tokens: int = 0


class ToolCall(BaseModel):
    """Tool call from agent."""
    tool_name: str
    tool_input: Dict[str, Any]


class LLMProvider:
    """Abstract LLM provider."""
    
    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Complete a prompt."""
        raise NotImplementedError


class MockLLMProvider(LLMProvider):
    """Mock LLM provider for testing."""
    
    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Mock completion."""
        # Simple mock response
        return {
            "content": "Mock response",
            "finish_reason": "stop",
            "tokens": 10,
        }


class OpenAIProvider(LLMProvider):
    """OpenAI LLM provider."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
    
    async def complete(
        self,
        messages: List[Dict[str, str]],
        model: str,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Complete using OpenAI."""
        try:
            import openai
            
            client = openai.AsyncOpenAI(api_key=self.api_key)
            
            # Prepare tool definitions for OpenAI
            tool_definitions = None
            if tools:
                tool_definitions = [
                    {
                        "type": "function",
                        "function": {
                            "name": tool["name"],
                            "description": tool.get("description", ""),
                            "parameters": tool.get("input_schema", {}),
                        },
                    }
                    for tool in tools
                ]
            
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens or 1000,
                tools=tool_definitions,
            )
            
            choice = response.choices[0]
            
            result = {
                "content": choice.message.content or "",
                "finish_reason": choice.finish_reason,
                "tokens": response.usage.total_tokens if response.usage else 0,
            }
            
            # Check for tool calls
            if choice.message.tool_calls:
                tool_calls = []
                for tool_call in choice.message.tool_calls:
                    tool_calls.append({
                        "tool_name": tool_call.function.name,
                        "tool_input": json.loads(tool_call.function.arguments),
                        "call_id": tool_call.id,
                    })
                result["tool_calls"] = tool_calls
            
            return result
        except Exception as e:
            logger.error(f"OpenAI completion error: {str(e)}")
            raise


class AgentExecutor:
    """Executes agent workflows with tool calling."""
    
    def __init__(
        self,
        agent_id: str,
        system_instructions: str,
        model: str = "gpt-4",
        temperature: float = 0.7,
        max_iterations: int = 10,
        max_tokens: Optional[int] = None,
        llm_provider: Optional[LLMProvider] = None,
    ):
        self.agent_id = agent_id
        self.system_instructions = system_instructions
        self.model = model
        self.temperature = temperature
        self.max_iterations = max_iterations
        self.max_tokens = max_tokens
        self.llm_provider = llm_provider or MockLLMProvider()
        self.state = AgentState(agent_id=agent_id)
        self.tool_registry = get_tool_registry()
    
    def add_memory(self, key: str, value: Any):
        """Add to agent memory."""
        self.state.memory[key] = value
    
    def get_memory(self, key: str) -> Any:
        """Get from agent memory."""
        return self.state.memory.get(key)
    
    async def execute(
        self,
        user_input: str,
        tools: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Execute agent with user input."""
        logger.info(f"Starting agent execution: {self.agent_id}")
        
        # Initialize conversation
        self.state.conversation_history = [
            AgentMessage(role="system", content=self.system_instructions),
            AgentMessage(role="user", content=user_input),
        ]
        
        # Use all tools if not specified
        if tools is None:
            tools = self.tool_registry.list_tools()
        
        # Execution loop
        final_response = None
        for iteration in range(self.max_iterations):
            self.state.iteration_count += 1
            
            # Get LLM response
            messages = [
                {"role": msg.role, "content": msg.content}
                for msg in self.state.conversation_history
            ]
            
            try:
                response = await self.llm_provider.complete(
                    messages=messages,
                    model=self.model,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    tools=tools,
                )
            except Exception as e:
                logger.error(f"LLM completion error: {str(e)}")
                return {
                    "status": "error",
                    "error": str(e),
                    "iterations": self.state.iteration_count,
                }
            
            self.state.total_tokens += response.get("tokens", 0)
            
            # Check for tool calls
            if response.get("tool_calls"):
                # Process tool calls
                tool_results = []
                for tool_call in response["tool_calls"]:
                    tool_result = await self._execute_tool(tool_call)
                    tool_results.append(tool_result)
                
                # Add assistant response
                self.state.conversation_history.append(
                    AgentMessage(role="assistant", content=response["content"])
                )
                
                # Add tool results
                for tool_result in tool_results:
                    self.state.conversation_history.append(
                        AgentMessage(role="tool", content=json.dumps(tool_result))
                    )
            else:
                # Final response
                final_response = response["content"]
                self.state.conversation_history.append(
                    AgentMessage(role="assistant", content=final_response)
                )
                break
        
        Metrics.record("agent_iterations", self.state.iteration_count)
        Metrics.record("agent_tokens", self.state.total_tokens)
        
        return {
            "status": "completed",
            "response": final_response or response.get("content", ""),
            "iterations": self.state.iteration_count,
            "tokens": self.state.total_tokens,
        }
    
    async def _execute_tool(self, tool_call: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single tool call."""
        tool_name = tool_call.get("tool_name")
        tool_input = tool_call.get("tool_input", {})
        
        logger.info(f"Executing tool: {tool_name}")
        
        try:
            result = await self.tool_registry.execute_tool(tool_name, tool_input)
            
            return {
                "tool_name": tool_name,
                "status": "success",
                "result": result,
            }
        except Exception as e:
            logger.error(f"Tool execution failed: {tool_name}: {str(e)}")
            return {
                "tool_name": tool_name,
                "status": "error",
                "error": str(e),
            }


def get_llm_provider(api_key: Optional[str] = None) -> LLMProvider:
    """Get LLM provider based on configuration."""
    from app.config import settings
    
    if api_key or settings.openai_api_key:
        try:
            return OpenAIProvider(api_key or settings.openai_api_key)
        except Exception as e:
            logger.warning(f"Failed to create OpenAI provider: {str(e)}, using mock")
            return MockLLMProvider()
    
    return MockLLMProvider()
