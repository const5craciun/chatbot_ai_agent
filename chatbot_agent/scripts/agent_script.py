# This script creates the AI agent and calls both the LLM and tools. 
# It adds a bit to the agent's prompt and keeps the session open until you write 'exit'
# The function is called in chatbot.ipynb and tests_chatbot.ipynb        
# !!! To run on your machine, a hugging face token is required. !!!

import os
from huggingface_hub import login
from sqlalchemy import text
from datetime import datetime
from smolagents import CodeAgent, InferenceClientModel, LogLevel, tool
from scripts.agent_tools.tools import aggregate_metric_simple_where, aggregate_with_grouping, plot_trend
from scripts.utils.tool_logger import log_tool_usage, clear_tool_log, get_tool_log


login(os.getenv('hf_token')) # this is hugging face token, make sure you generate one in the HF website

def chatbot_interaction(predefined_questions=None):


    model = InferenceClientModel(
        model_id="meta-llama/Llama-3.3-70B-Instruct",
        temperature=0.0,          # to keep it deterministic
        top_p=0.9,
        max_tokens=2048)          # maybe i change it later

    agent = CodeAgent(
        tools=[aggregate_metric_simple_where, aggregate_with_grouping, plot_trend],
        model=model,
        additional_authorized_imports=["matplotlib.pyplot", "pandas"],
        planning_interval=3,
        verbosity_level=LogLevel.ERROR, # comment this out if you want to see the CoT, reasoning or steps taken
    )

    agent.prompt_templates["system_prompt"] += """

        DATABASE CONSTRAINT:
        You must ONLY query the table `marketing_data`.
        No other tables exist.
        Never reference any other table name.
        All SQL queries must operate exclusively on marketing_data.
        If the user asks for 'last', take the last information from marketing_data. Example: User: What is the last year? You would take the last year present in marketing_data table.
        """
    
    STOP_WORDS = {"exit"} # string to stop the conversation
    conversation = [] # to keep a context window
    interaction_log = list()
    questions_iter = iter(predefined_questions) if predefined_questions else None
    
    while True:
        if questions_iter:
            try:
                user_input = next(questions_iter)
                print("User:", user_input)
            except StopIteration:
                break
        else:
            user_input = input(f"Write 'exit' to end the chat. \n Ask your question: ")
        
        if user_input.lower() in STOP_WORDS:
            print("Agent: Conversation ended.")
            break
        
        conversation.append(f"User: {user_input}")
        full_prompt = "\n".join(conversation)
        response = agent.run(full_prompt, stream=False)
        
        print(f"Agent response for your question '{user_input}' is :",response)
        conversation.append(f"Agent: {response}")
    
        
        interaction_log.append({
            "user_question": user_input,
            "agent_response": response,
            "tools_used": get_tool_log()
        })
        
        clear_tool_log()

    return interaction_log

