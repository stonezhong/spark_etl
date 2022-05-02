import json
import jinja2

# read file as string
# json_encoded: if True, content is json encoded
def file(filename, json_encoded=False):
    if json:
        with open(filename, "rt") as f:
            return json.dumps(f.read())[1:-1]

    with open(filename, "rt") as f:
        return f.read()


class GenTemplate:
    def __init__(self):
        self.env = jinja2.Environment()
        self.env.filters["file"] = file
    
    def render(self, filename, **context):
        with open(filename, "rt") as cf:
            content = cf.read()
        tmpl = self.env.from_string(content)
        return tmpl.render(**context)
    
