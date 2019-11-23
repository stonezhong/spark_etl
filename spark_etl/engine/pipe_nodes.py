from collections import defaultdict

class PipeNode(object):
    node_dict = {}

    def __init__(self, id, input_names=['input'], output_names=['output']):
        self.id = id
        self.input_names = input_names
        self.output_names = output_names
        
        self.output_dict = defaultdict(list)
        self.input_dict  = defaultdict(list)

        self.connection_dict = defaultdict(list)
        # output will feed into node foo's input named with 'A'
        # {
        #     'output': [('foo': 'A')]
        # }
    
    def set_input(self, input_name, data):
        if input_name not in self.input_names:
            raise ValueError("{} is not a valid input".format(input_name))
        self.input_dict[input_name].append(data)


    def set_input_list(self, input_name, data_list):
        if input_name not in self.input_names:
            raise ValueError("{} is not a valid input".format(input_name))
        self.input_dict[input_name].extend(data_list)


    def set_output(self, output_name, data):
        if output_name not in self.output_names:
            raise ValueError("{} is not a valid output".format(output_name))
        self.output_dict[output_name].append(data)


    def run(self):
        # derived class to override
        raise NotImplementedError()

    def flush_output(self):
        other_pipe_node_ids = set()

        for output_name, notify_list in self.connections.items():
            data_list = self.output_dict[output_name]
            if len(data_list) > 0:
                for other_pipe_node_id, other_pipe_node_input_name in notify_list.items():
                    other_pipe_node_ids.add(other_pipe_node_id)
                    other_pipe_node = self.node_dict[other_pipe_node_id]
                    other_pipe_node.set_input_list(other_pipe_node_input_name, data_list)

        for other_pipe_node_id in other_pipe_node_ids:
            other_pipe_node = self.node_dict[other_pipe_node_id]
            other_pipe_node.run()
            

    def connect(self, output_name, other_pipe_node_id, other_pipe_node_input_name):
        if output_name not in self.connection_dict:
            self.connection_dict[output_name] = []
        
        self.connection_dict[output_name].append((other_pipe_node_id, other_pipe_node_input_name, ))


class TeePipeNode(PipeNode):
    def __init__(self, id, output_names):
        super(TeePipeNode, self).__init__(id, output_names=output_names)
    
    def run(self):
        df = self.input_dict['input']
        for output_name in output_names:
            self.set_putput(output_name, df)
    

class TransformerPipeNode(PipeNode):
    def __init__(self, id, transformer, input_names=['input'], output_names=['output']):
        super(TransformerPipeNode, self).__init__(id, input_names=input_names, output_names=output_names)
        self.transformer = transformer
    
    def run(self):
        r = self.transformer.transform(**self.input_dict)
        if not isinstance(r, dict):
            r = {'output': r}
        
        for output_name, df in r.items():
            self.set_output(output_name, df)
