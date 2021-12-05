# etl

It is a command line tool to build, deploy and run spark applications.

# configurations
configurations are stored in a json file, by default, filename is `config.json`, you can override it with option `-c`.

<table>
<tr>
<th>option name</th>
<th>description</th>
<th>example</th>
</tr>
<tr>
<td>apps_dir</td>
<td>Specify the directory that contains all the applications. If missing, using "apps"</td>
<td>"apps"</td>
</tr>
<tr>
<td>builds_dir</td>
<td>Specify the directory that stores application artifacts generated from build, if missing, using ".builds"</td>
<td>".builds"</td>
</tr>
<tr>
<td>common_requirements</td>
<td>A requirement.txt file, it's content will be appended to each application specific requirement.txt file. This file is optional.</td>
<td>".builds"</td>
</tr>
<tr>
<td>profiles_dir</td>
<td>A directory that has all the profiles. If missing, using ".profiles"</td>
<td>".profiles"</td>
</tr>
<tr>
<td>artifacts_dir</td>
<td>A directory that has all the artifacts. If missing, using ".artifacts"</td>
<td>".artifacts"</td>
</tr>
</table>

# build an app
```bash
# build application "myapp"
etl -a build -p myapp
```

# deploy an app
```bash
# deploy application "myapp" with profile "home_hdfs"
etl -a deploy -p myapp -f home_hdfs
```

# run an app
```bash
# run application "myapp" with profile "home_hdfs", input read from ~/foo.json
etl -a run -p myapp -f home_hdfs --run-args ~/foo.json
```
