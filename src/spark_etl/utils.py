import io
import subprocess
import time
from contextlib import redirect_stdout, redirect_stderr
import code
from datetime import datetime
import readline

from termcolor import colored, cprint

CLI_REQUEST_NAME = "cli-request.json"
CLI_RESPONSE_NAME = "cli-response.json"

def handle_pwd(spark, user_input, channel):
    channel.write_json(
        spark,
        CLI_RESPONSE_NAME,
        {
            "status": "ok",
            "output": os.getcwd()
        }
    )

def handle_bash(spark, user_input, channel):
    cmd_buffer = '\n'.join(user_input['lines'])
    f = io.StringIO()
    with redirect_stdout(f):
        p = subprocess.run(cmd_buffer, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    channel.write_json(
        spark,
        CLI_RESPONSE_NAME,
        {
            "status": "ok",
            "exit_code": p.returncode,
            "output": p.stdout.decode('utf-8'),
        }
    )

def handle_python(spark, user_input, console, channel):
    source = '\n'.join(user_input['lines'])
    stdout_f = io.StringIO()
    stderr_f = io.StringIO()
    with redirect_stdout(stdout_f):
        with redirect_stderr(stderr_f):
            console.runsource(source, symbol="exec")

    channel.write_json(
        spark, 
        CLI_RESPONSE_NAME,
        {
            "status": "ok",
            "output": stdout_f.getvalue() + "\n" + stderr_f.getvalue() ,
        }
    )

class PySparkConsole(code.InteractiveInterpreter):
    def __init__(self, locals=None):
        super(PySparkConsole, self).__init__(locals=locals)


class CLIHandler:
    def __init__(self, client_channel, is_job_active, handlers):
        self.client_channel = client_channel
        if is_job_active is None:
            self.is_job_active = lambda : True
        else:
            self.is_job_active = is_job_active
        
        self.handlers = handlers
        self.last_job_ok_time = None


    def loop(self):
        # line_mode can be "bash", "python" or "OFF"
        # When line_mode is OFF, you need send explicitly run @@bash or @@python
        # to submit a block of code to server
        # When line_mode is bash, each line is a bash script
        # when line_mode is python, each line is a python script
        line_mode = "off"

        # if is_waiting_for_response is True, we need to pull server for cli-response.json
        # if is_waiting_for_response is False, we are free to enter new command
        is_waiting_for_response = True
        # command line buffer
        cli_lines = []
        cli_wait_prompt = "-/|\\"
        cli_wait_prompt_idx = 0
        log_filename = None
        # commands
        # @@log           -- all the output will be written to the log file as well
        #                    by default there is not log
        # @@nolog         -- turn off log
        # @@clear         -- clear command line buffer
        # @@load          -- load a script from local file and append to command buffer
        # @@bash          -- submit a bash script
        # @@python        -- submit a python script
        # @@show          -- show the command buffer
        # @@pwd           -- show driver's current directory
        # @@quit          -- quit the cli console

        command = None
        while True:
            handle_server_ask(self.client_channel, self.handlers)
            if not is_waiting_for_response:
                if line_mode == "off":
                    prompt = "> "
                elif line_mode == "bash":
                    prompt = "bash> "
                else:
                    prompt = "python> "

                command = input(prompt)

                if command == "@@quit":
                    self.client_channel.write_json(CLI_REQUEST_NAME, {"type": "@@quit"})
                    is_waiting_for_response = True
                    continue

                if command == "@@pwd":
                    self.client_channel.write_json(CLI_REQUEST_NAME, {"type": "@@pwd"})
                    is_waiting_for_response = True
                    continue

                if command == "@@bash":
                    self.client_channel.write_json(
                        CLI_REQUEST_NAME,
                        {
                            "type": "@@bash",
                            "lines": cli_lines
                        }
                    )
                    is_waiting_for_response = True
                    cli_lines = []
                    continue

                if command == "@@python":
                    self.client_channel.write_json(
                        CLI_REQUEST_NAME,
                        {
                            "type": "@@python",
                            "lines": cli_lines
                        }
                    )
                    is_waiting_for_response = True
                    cli_lines = []
                    continue

                if command.startswith("@@mode"):
                    cmds = command.split(" ")
                    if len(cmds) != 2 or cmds[1] not in ("off", "bash", "python"):
                        print("Usage:")
                        print("@@mode off")
                        print("@@mode python")
                        print("@@mode bash")
                    else:
                        line_mode = cmds[1]
                    continue

                if command.startswith("@@log"):
                    cmds = command.split(" ")
                    if len(cmds) != 2:
                        print("Usage:")
                        print("@@log <filename>")
                    else:
                        log_filename = cmds[1]
                    continue

                if command.startswith("@@load"):
                    cmds = command.split(" ")
                    if len(cmds) != 2:
                        print("Usage:")
                        print("@@load <filename>")
                    else:
                        try:
                            with open(cmds[1], "rt") as load_f:
                                for line in load_f:
                                    cli_lines.append(line.rstrip())
                        except Exception as e:
                            print(f"Unable to read from file: {str(e)}")
                    continue

                if command == "@@clear":
                    cli_lines = []
                    continue

                if command == "@@show":
                    for line in cli_lines:
                        print(line)
                    print()
                    continue

                # for any other command, we will append to the cli buffer
                if line_mode == "off":
                    cli_lines.append(command)
                else:
                    self.client_channel_write_json(
                        CLI_REQUEST_NAME,
                        {
                            "type": "@@" + line_mode,
                            "lines": [ command ]
                        }
                    )
                    is_waiting_for_response = True
            else:
                if self.client_channel.has_json(CLI_RESPONSE_NAME):
                    response = self.client_channel.read_json(CLI_RESPONSE_NAME)
                    self.client_channel.delete_json(CLI_RESPONSE_NAME)
                    # print('#################################################')
                    # print('# Response                                      #')
                    # print(f"# status   : {response['status']}")
                    # if 'exit_code' in response:
                    #     print(f"# exit_code: {response['exit_code']}")
                    # print('#################################################')
                    cprint(response['output'], 'green', 'on_red')
                    if log_filename is not None:
                        try:
                            with open(log_filename, "a+t") as log_f:
                                print(response['output'], file=log_f)
                                print("", file=log_f)
                        except Exception as e:
                            print(f"Unable to write to file {log_filename}: {str(e)}")

                    if command == "@@quit":
                        break

                    if self.is_job_active():
                        self.last_job_ok_time = datetime.utcnow()
                        is_waiting_for_response = False
                    else:
                        print("Job quit unexpectedly!")
                        break
                else:
                    time.sleep(1) # do not sleep too long since this is an interactive session
                    now = datetime.utcnow()
                    if self.last_job_ok_time is None or (now - self.last_job_ok_time).total_seconds() >= 10:
                        if self.is_job_active():
                            self.last_job_ok_time = now
                        else:
                            print("Job quit unexpectedly!")
                            break

                    print(f"\r{cli_wait_prompt[cli_wait_prompt_idx]}\r", end="")
                    cli_wait_prompt_idx = (cli_wait_prompt_idx + 1) % 4


def cli_main(spark, args, sysops={}):
    channel = sysops['channel']
    console = PySparkConsole(locals={'spark': spark, 'sysops': sysops})

    channel.write_json(
        spark, 
        CLI_RESPONSE_NAME,
        {
            "status": "ok",
            "output": "Welcome to OCI Spark-CLI Interface",
        }
    )

    while True:
        if not channel.has_json(spark, CLI_REQUEST_NAME):
            time.sleep(1)
            continue

        user_input = channel.read_json(spark, CLI_REQUEST_NAME)
        channel.delete_json(spark, CLI_REQUEST_NAME)

        if user_input["type"] == "@@quit":
            channel.write_json(
                spark,
                CLI_RESPONSE_NAME,
                {
                    "status": "ok",
                    "output": "Server quit gracefully",
                }
            )
            break
        if user_input["type"] == "@@pwd":
            handle_pwd(spark, user_input, channel)
            continue
        if user_input["type"] == "@@bash":
            handle_bash(spark, user_input, channel)
            continue
        if user_input["type"] == "@@python":
            handle_python(spark, user_input, console, channel)
            continue
    return {"status": "ok"}

SERVER_TO_CLIENT_REQUEST    = "stc-request.json"
SERVER_TO_CLIENT_RESPONSE   = "stc-response.json"

class RemoteCallException(Exception):
    def __init__(self, status, exception_msg=None, exception_name=None):
        if status == 'nohandler':
            msg = 'remote call failed: no handler'
        elif status == 'exception':
            msg = f'remote call failed: exception, name="{exception_name}", msg="{exception_msg}"'
        else:
            msg =f'remote call failed: something is wrong'

        super(RemoteCallException, self).__init__(msg)
        self.msg = msg
        self.status = status
        self.exception_msg = exception_msg
        self.exception_name = exception_name


def server_ask_client(spark, channel, content, timeout=600, check_interval=5):
    """Called by server, ask a question and wait for answer
    """
    channel.write_json(spark, SERVER_TO_CLIENT_REQUEST, content)
    start_time = datetime.utcnow()
    while True:
        if channel.has_json(spark, SERVER_TO_CLIENT_RESPONSE):
            response = channel.read_json(spark, SERVER_TO_CLIENT_RESPONSE)
            channel.delete_json(spark, SERVER_TO_CLIENT_RESPONSE)
            if response['status'] == 'ok':
                return response['answer']
            elif response['status'] == 'nohandler':
                raise RemoteCallException("nohandler")
            elif response['status'] == 'exception':
                exception_name = response.get('exception_name')
                exception_msg = response.get('exception_msg')
                raise RemoteCallException(
                    "exception", 
                    exception_name = response.get('exception_name'),
                    exception_msg = response.get('exception_msg')
                )
            else:
                raise RemoteCallException(None)


        if (datetime.utcnow() - start_time).total_seconds() >= timeout:
            raise Exception("Ask is not answered: timed out")
        else:
            time.sleep(check_interval)


def handle_server_ask(channel, handlers):
    """Called by job submitter to answer ask from server with handlers
    """
    if handlers is None:
        return
        
    if not channel.has_json(SERVER_TO_CLIENT_REQUEST):
        return

    content = channel.read_json(SERVER_TO_CLIENT_REQUEST)
    channel.delete_json(SERVER_TO_CLIENT_REQUEST)

    for handler in handlers:
        try:
            handled, out = handler(content)
            if handled:
                channel.write_json(
                    SERVER_TO_CLIENT_RESPONSE,
                    {
                        'status': 'ok',
                        'answer': out
                    }
                )
            return
        except Exception as e:
                channel.write_json(
                    SERVER_TO_CLIENT_RESPONSE,
                    {
                        'status': 'exception',
                        "exception_name": e.__class__.__name__,
                        "exception_msg": str(e)
                    }
                )
                return

    channel.write_json(
        SERVER_TO_CLIENT_RESPONSE,
        {
            'status': 'nohandler'
        }
    )
    return

