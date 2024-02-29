import json
import re
from time import sleep

import requests
from flask import Flask, Response, stream_with_context, request
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

@app.route('/aiwrite', methods=['POST'])
@cross_origin()
def process_request():
    try:
        # Check the request content type to ensure it is "application/json"
        if request.headers.get('Content-Type') != 'application/json':
            return json.dump({"msg": "Bad Request", "response": 500, "results":"Unsupported Media Type"},ensure_ascii = False)

        data = request.get_json()

        # Check if 'input' and 'prompt' are provided
        if 'input' not in data:
            return json.dumps({"msg": "Bad Request", "response": 500, "results":"没有input参数"},ensure_ascii = False)
        elif 'prompt' not in data:
            return json.dumps({"msg": "Bad Request", "response": 500, "results":"没有prompt参数"},ensure_ascii = False)

        input_text = data['input']
        prompt = data['prompt']

        if 'stream' not in data:
            stream = False
        else:
            stream = data.get('stream')

        remind = ""
        if 'remind' in data:
            remind = data.get('remind')


        # Get 'rewrite_style' if provided, otherwise set to None
        rewrite_key = {0:"通用改写",1:"新闻记者",2:"动漫主播",3:"学生"}
        rewrite_style = ""
        try:
            rewrite_code = data.get('rewrite_style')
            rewrite_style=rewrite_key.get(rewrite_code)
        except:
            print("wu")

        prompt_message = ""
        # Generate the prompt based on the input, prompt, and rewrite_style
        if prompt == 1:
            prompt_message = f"主题是：{input_text}。请根据指定的主题返回内容提纲。"
        elif prompt == 2:
            prompt_message = f"主题是：{input_text}。请根据指定的主题写一篇文章，字数在500字以上。"
        elif prompt == 3:
            prompt_message = f"以下为内容开头：{input_text}。以与提供的上下文一致的方式继续用中文写一篇文章。"
        elif prompt == 4:
            if remind == "":
                prompt_message = f"文章为：{input_text}。请对这篇文章进行润色，使其表达更加准确，内容更加精美。"
            else:
                prompt_message = f"文章为：{input_text}。要求为：{remind}。请根据要求对这篇文章进行润色，使其表达更加准确，内容更加精美。"

        elif prompt == 5:
            if rewrite_style != "":
                if remind == "":
                    prompt_message = f"文章全文改写。文章全文为：{input_text}。请以{rewrite_style}的口吻将该文章改写。"
                else:
                    prompt_message = f"文章全文改写。文章全文为：{input_text}。改写要求为：{remind}。请根据要求以{rewrite_style}的口吻将该文章进行改写。"
            else:
                if remind == "":
                    prompt_message = f"文章全文改写。文章全文为：{input_text}。请将以上文本进行改写。"
                else:
                    prompt_message = f"文章全文改写。文章全文为：{input_text}。改写要求为：{remind}。请根据要求将该文章进行改写。"
                    print(prompt_message)

        else:
            return json.dumps({"msg": "Invalid prompt value", "response": 500, "result":"please choose prompt value 0-5"},ensure_ascii = False)

        # Here you can implement the logic to generate the appropriate response based on the request parameters.
        # For simplicity, we'll just echo the input parameters.
        print(prompt_message)
        url = 'http://10.211.25.28:3000/v1/chat/completions'  # 替换为你的目标 URL

        # 定义要发送的数据（可以是字典、JSON 等格式）
        data = {
                    "model": "SparkDesk",
                    "messages":  [
                        {
                            "role": "user",
                            "content": prompt_message
                        }
                    ],

                    "temperature": 0.7,
                    "top_p": 1,
                    "max_tokens": 4096,
                    "stop": [
                        "string"
                    ],
                    "user": "pdmi"
                }

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer sk-9aKmaj8vpFGB9OlRCdFaA7Dc352a40158d9578Bc29A0Ac6e"
        }

        response = requests.post(url, json=data, headers=headers)  # 使用 JSON 格式发送数据

        if stream:
            data = {
                "model": "SparkDesk",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt_message
                    }
                ],
                "temperature": 0.7,
                "top_p": 1,
                "max_tokens": 4096,
                "stop": [
                    "string"
                ],
                "user": "pdmi",
                "stream": True,
            }
            response = requests.post(url, json=data, headers=headers)
            #pattern = r'data:\s*(\{.*?"id"\s*:\s*"[^"]*"\s*,\s*"model"\s*:\s*"[^"]*"\s*,\s*"choices"\s*:\s*\[.*?\]\s*\})'
            pattern = r'data:\s*({.*?"id"\s*:\s*"[^"]*"\s*,\s*"object"\s*:\s*"[^"]*"\s*,\s*"created"\s*:\s*\d+\s*,\s*"model"\s*:\s*"[^"]*"\s*,\s*"choices"\s*:\s*\[.*?\]\s*})'

            def generate_data():
                partial_data = ""
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        try:
                            # 将数据块解码为字符串
                            chunk_str = chunk.decode("utf8", 'ignore')
                            #print(chunk_str)

                        except UnicodeDecodeError as e:
                            # 处理无法解码的数据块，可以选择跳过或记录错误
                            print(f"UnicodeDecodeError: {e}")
                            continue
                        #
                        # # 将数据块追加到暂存字符串
                        partial_data += chunk_str
                        match = re.search(pattern, partial_data)
                        # 查找是否有完整的 JSON 数据
                        while match:
                            _, data_block, partial_data = re.split(pattern, partial_data, maxsplit=1)
                            match = re.search(pattern, partial_data)
                            json_block = json.loads(data_block)
                            choices = json_block.get("choices", [])
                            try:
                                content = choices[0]['delta']['content']
                            except:
                                content = ""
                            yield content + "\n"
                            sleep(0.1)

            if response.status_code == 200:
                return Response(stream_with_context(generate_data()), mimetype='text/event-stream')
            else:
                return json.dumps({"msg": "error", "response": 400, "results": response.text}, ensure_ascii=False)


        response_content = ""
        # 检查响应状态码
        if response.status_code == 200:
            # 解析响应内容
            response_data = response.json()  # 如果服务器返回 JSON 数据
            response_content = response_data['choices'][0]['message']['content']
            print(response_content)
        else:
            return json.dumps({"msg": "error", "response": 400, "results": response.text}, ensure_ascii=False)
        return json.dumps({"msg": "ok", "response": 200, "results": response_content}, ensure_ascii=False)
    except Exception as e:
        print(e)
        return json.dumps({"msg": "Internal Server Error", "response": 500, "results": "请重试"},ensure_ascii = False)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=9000)