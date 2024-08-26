import gradio as gr
from transformers import TextIteratorStreamer
from threading import Thread

min_tokens = 64
max_tokens = 1024
instructions = ""

if gr.NO_RELOAD:
    from unsloth import FastLanguageModel

    model_name = "model"
    max_seq_length = 1024 # Choose any! We auto support RoPE Scaling internally!
    dtype = None # None for auto detection. Float16 for Tesla T4, V100, Bfloat16 for Ampere+
    load_in_4bit = True # Use 4bit quantization to reduce memory usage. Can be False.

    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name = model_name, # YOUR MODEL YOU USED FOR TRAINING
        max_seq_length = max_seq_length,
        dtype = dtype,
        load_in_4bit = load_in_4bit,
    )
    FastLanguageModel.for_inference(model) # Enable native 2x faster inference
    tokenizer.pad_token = '<|reserved_special_token_250|>'
    tokenizer.pad_token_id = 128255

# Define the function to stream output to Gradio
def generate_stream(message, history):
    prompt = message.strip()
    # Don't generate if prompt is empty
    if prompt == "":
        yield "Please type a message to generate response."
        return
    
    print("Prompt: "+prompt)
    history.append(prompt)
    # Prepare the input messages
    msgs = [
        {"role": "system", "content": instructions},
        {"role": "user", "content": prompt}
    ]

    # Tokenize and prepare the inputs
    inputs = tokenizer.apply_chat_template(
        msgs,
        tokenize=True,
        add_generation_prompt=True,
        return_tensors="pt"
    ).to("cuda")

    attention_mask = (inputs != tokenizer.pad_token_id).int()

    # Set up the TextStreamer
    text_streamer = TextIteratorStreamer(tokenizer, skip_prompt=True, skip_special_tokens=True)
    generate_kwargs = dict(
        input_ids = inputs, 
        attention_mask = attention_mask, 
        streamer = text_streamer, 
        min_new_tokens = min_tokens, 
        max_new_tokens = max_tokens,
        temperature = 0.8,
        repetition_penalty = 1.25,
        top_p = 0.9,
        use_cache = True
    )

    t = Thread(target=model.generate, kwargs=generate_kwargs)
    t.start()

    partial_message = ""
    history.append(partial_message)
    for new_token in text_streamer:
        if new_token != '<':
            partial_message += new_token
            history[-1] = partial_message
            yield partial_message

with gr.Blocks(theme="gradio/soft") as demo:
    gr.Markdown("<center><h1>Reddit Chatbot</h1></center>")
    gr.ChatInterface(
        generate_stream, 
        chatbot = gr.Chatbot(label="Chatbot", scale=1, height="60vh"),
        cache_examples=True,
    )

if __name__ == "__main__":
    demo.launch()
