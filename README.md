# Reddit Chatbot

This project focuses on finetuning the Llama 3 8B Instruct model on Reddit submissions and comments. Uses [Unsloth](https://github.com/unslothai/unsloth) for better training efficiency.

## Dataset
This [torrent](https://academictorrents.com/details/20520c420c6c846f555523babc8c059e9daa8fc5) contains submissions and comments of the top 40,000 subreddits from June 2005 to December 2023, which are represented in separate zstandard files (more information [here](https://www.reddit.com/r/pushshift/comments/1akrhg3/separate_dump_files_for_the_top_40k_subreddits/)). ``build_db.py`` extracts the compressed files and dumps them into a SQLite database. ``build_train.py`` creates input-output pairs for the train dataset based on the submission, comments, and replies. These pairs are then written to a JSON file.

## Usage
Install the required libraries:
```
./install.sh
```
To extract the content of the submission and comment files to a SQLite database, run
```
python build_db.py <submission zst file> <database file>
python build_db.py <comment zst file> <database file>
```
Note: both submission and comment ``.zst`` files are required to build the train dataset!

Next, to create the train dataset, run
```
python build_train.py <database file> <json file>
```

## Relevant Resources
* [Unsloth Github](https://github.com/unslothai/unsloth)
* [Llama 3.1 Base 8b Model](https://huggingface.co/unsloth/Meta-Llama-3.1-8B-bnb-4bit)
* [Llama 3.1 Instruct 8b Model](https://huggingface.co/unsloth/Meta-Llama-3.1-8B-Instruct-bnb-4bit)
* [Llama-3.1 8b + Unsloth 2x faster finetuning](https://colab.research.google.com/drive/1Ys44kVvmeZtnICzWz0xgpRnrIOjZAuxp?usp=sharing)
* [Llama-3 8b Instruct Unsloth 2x faster finetuning](https://colab.research.google.com/drive/1XamvWYinY6FOSX9GLvnqSjjsNflxdhNc?usp=sharing)
* [Supervised Fine-tuning Trainer](https://huggingface.co/docs/trl/sft_trainer)
* [Loading dataset from different formats](https://huggingface.co/docs/datasets/en/loading)
* [Loading dataset from JSON file](https://huggingface.co/docs/datasets/en/loading#json)
* [Loading dataset from SQLite3 database](https://huggingface.co/docs/datasets/main/en/tabular_load#sqlite)
* [Llama 3.1 Instruct Prompt Format](https://llama.meta.com/docs/model-cards-and-prompt-formats/llama3_1/#llama-3.1-instruct)
* [Subreddit comments/submissions 2005-06 to 2023-12](https://academictorrents.com/details/20520c420c6c846f555523babc8c059e9daa8fc5)
