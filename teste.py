from openai import OpenAI
import os

client = OpenAI(
    api_key= os.getenv("GPT_API_KEY")
)

completion = client.chat.completions.create(
    model="gpt-4o-mini",
    store=True,
    messages=[
    {"role": "user", "content": "write a haiku about ai with 10 words" },
    ] 
)

content = completion.choices[0].message.content

print(content)
