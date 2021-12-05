import argparse
import json
import importlib
import os
import glob
from jinja2 import Template

from spark_etl import Application, Build

class Config:
    def __init__(self, config):
        self.config = config

    @classmethod
    def get(cls, config_filename):
        if os.path.isfile(config_filename):
            with open(config_filename, "r") as f:
                config = json.load(f)
        else:
            config = { }
        
        # TODO: validate schema
        return Config(config)
    
    @property
    def apps_dir(self):
        return self.config.get("apps_dir", "apps")
    
    @property
    def builds_dir(self):
        return self.config.get("builds_dir", ".builds")
    
    @property
    def default_libs(self):
        common_req_filename = self.config.get("common_requirements")
        if common_req_filename is None:
            return []
        
        ret = []
        with open(common_req_filename, "rt") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                l = line.strip()
                if len(l) > 0:
                    ret.append(l)
        return ret
    
    @property
    def profiles_dir(self):
        return self.config.get("profiles_dir", ".profiles")


    @property
    def artifacts_dir(self):
        return self.config.get("artifacts_dir", ".artifacts")
    
    def get_app_dir(self, app_name):
        return os.path.join(self.apps_dir, app_name)
    
    def get_build_dir(self, app_name):
        return os.path.join(self.builds_dir, app_name)

    def get_profile(self, profile_name):
        artifacts = load_artifacts(self.artifacts_dir)
        profile_filename = os.path.join(self.profiles_dir, f"{profile_name}.json")

        with open(profile_filename, "rt") as cf:
            profile_template_text = cf.read()
        
        profile_template = Template(profile_template_text)
        profile_text = profile_template.render(artifacts=artifacts)

        profile = json.loads(profile_text)
        return Profile(profile)

class Profile:
    def __init__(self, profile):
        self.profile = profile
        self._deployer = None
        self._job_submitter = None
    
    @property
    def deploy_base(self):
        return self.profile['deploy_base']
    
    def get_deploy_dir(self, app_name):
        return os.path.join(self.deploy_base, app_name)
    
    def get_deploy_version_dir(self, app_name, version):
        return os.path.join(self.deploy_base, app_name, version)

    
    @property
    def job_run_options(self):
        return self.profile.get("job_run_options", {})
    
    @property
    def deployer(self):
        if self._deployer is None:
            deployer_config = self.profile["deployer"]
            class_name  = deployer_config['class']
            module      = importlib.import_module('.'.join(class_name.split(".")[:-1]))
            klass       = getattr(module, class_name.split('.')[-1])
            args        = deployer_config.get("args", [])
            kwargs      = deployer_config.get("kwargs", {})
            self._deployer = klass(*args, **kwargs)
        return self._deployer

    
    @property
    def job_submitter(self):
        if self._job_submitter is None:
            job_submitter_config = self.profile["job_submitter"]
            class_name  = job_submitter_config['class']
            module      = importlib.import_module('.'.join(class_name.split(".")[:-1]))
            klass       = getattr(module, class_name.split('.')[-1])
            args    = job_submitter_config.get("args", [])
            kwargs  = job_submitter_config.get("kwargs", {})
            self._job_submitter = klass(*args, **kwargs)
        return self._job_submitter


def load_artifacts(root_dir):
    ret = {}
    if not os.path.isdir(root_dir):
        return ret
    for filename in os.listdir(root_dir):
        full_filename = os.path.join(root_dir, filename)
        if os.path.isdir(full_filename):
            ret[filename] = load_artifacts(full_filename)
        else:
            with open(full_filename, "rt") as f:
                ret[filename] = json.dumps(f.read())[1:-1]
    return ret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--action", required=True, choices=['build', 'deploy', 'run']
    )
    parser.add_argument(
         "-c", "--config-filename", help="Configuration file"
    )
    parser.add_argument(
        "-p", "--app-name", help="Application name"
    )
    parser.add_argument(
        "-f", "--profile-name", help="Profile name"
    )
    parser.add_argument(
        "-v", "--version", help="Application version"
    )
    parser.add_argument(
        "--run-args", help="Arguments for run, a filename to a json"
    )
    parser.add_argument(
        "--cli-mode",
        action="store_true",
        help="Using cli mode?"
    )
    args = parser.parse_args()
    
    config_filename = args.config_filename or "config.json"
    config = Config.get(config_filename)

    if args.action == "build":
        do_build(config, args)
    elif args.action == "deploy":
        do_deploy(config, args)
    elif args.action == "run":
        do_run(config, args)

    return


# build an application
def do_build(config, args):
    if args.app_name is None:
        raise Exception("Please specify app-name!")

    app_name    = args.app_name
    print(f"Building application: {app_name}")
    build_dir   = config.get_build_dir(app_name)
    app_dir     = config.get_app_dir(app_name)
    # cleanup the old build
    os.makedirs(build_dir, exist_ok=True)
    for f in glob.glob(f'{build_dir}/*'):
        os.remove(f)
    app = Application(app_dir)
    app.build(build_dir, default_libs=config.default_libs)
    print("Build application: done!")


def do_deploy(config, args):
    if args.app_name is None:
        raise Exception("Please specify app-name")
    if args.profile_name is None:
        raise Exception("Please specify profile-name")

    app_name        = args.app_name
    profile_name    = args.profile_name
    print(f"Deploy application: {args.app_name}, using profile {profile_name}")

    profile = config.get_profile(profile_name)
    build_dir       = config.get_build_dir(app_name)
    deploy_dir      = profile.get_deploy_dir(app_name)
    profile.deployer.deploy(build_dir, deploy_dir)
    print("Deploy application: done")


def do_run(config, args):
    if args.app_name is None:
        raise Exception("Please specify app-name")
    if args.profile_name is None:
        raise Exception("Please specify profile-name")

    app_name        = args.app_name
    profile_name    = args.profile_name

    build_dir       = config.get_build_dir(app_name)

    if args.version:
        version = args.version
    else:
        with open(os.path.join(build_dir, "manifest.json"), "r") as f:
            version = json.load(f)['version']

    print(f"Run application: {app_name}, version={version}, using profile {profile_name}")

    run_args = args.run_args
    if run_args is None:
        run_args_value = {}
    else:
        with open(run_args, "r") as f:
            run_args_value = json.load(f)

    profile = config.get_profile(profile_name)
    deploy_version_dir      = profile.get_deploy_version_dir(app_name, version)

    ret = profile.job_submitter.run(
        deploy_version_dir,
        options=profile.job_run_options,
        args=run_args_value,
        cli_mode=args.cli_mode
    )
    print("Run application: done!")
    print(f"return = {ret}")
