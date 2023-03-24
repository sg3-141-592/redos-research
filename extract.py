from dateutil import parser
import json
import luigi
import requests
import os
import regexploit
import tarfile
from io import BytesIO

class GetTopPypiPackages(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/top-packages.json')
    
    def run(self):
        top_package_data = requests.get("https://hugovk.github.io/top-pypi-packages/top-pypi-packages-30-days.min.json")
        with self.output().open("w") as outfile:
            outfile.write(json.dumps(top_package_data.json(), indent=4))

class GetPypiData(luigi.Task):

    PackageID = luigi.IntParameter()

    def requires(self):
        return GetTopPypiPackages()
    
    def output(self):
        return luigi.LocalTarget(f"data/pypi_{self.PackageID}")
    
    def run(self):
        with self.input().open("r") as i:
            item = json.load(i)["rows"][self.PackageID]
            pypi_data = requests.get(f"https://pypi.org/pypi/{item['project']}/json").json()
            latest_datetime = parser.parse("1900-03-24T22:11:08.536706Z")
            latest_release = "err"
            latest_url = "err"
            with self.output().open("w") as outfile:
                for release in pypi_data["releases"].keys():
                    for file in pypi_data["releases"][release]:
                        if file["packagetype"] == "sdist":
                            current_datetime = parser.parse(file["upload_time_iso_8601"])
                            if current_datetime > latest_datetime:
                                latest_datetime = current_datetime
                                latest_release = release
                                latest_url = file["url"]
                outfile.write(latest_url)

class DownloadPackage(luigi.Task):

    PackageID = luigi.IntParameter()

    def requires(self):
        return GetPypiData(self.PackageID)
    
    def output(self):
        return luigi.LocalTarget(
            f"data/path_{self.PackageID}.txt"
        )
    
    def run(self):
        with self.input().open("r") as i:
            url = i.read()
            response = requests.get(url, stream=True)
            t = tarfile.open(name=None, fileobj=response.raw)
            t.extractall(f"data/")
            output_path = os.path.commonprefix(t.getnames())
            t.close()
            with self.output().open("w") as outfile:
                outfile.write(f"data/{output_path}")

class AnalysePackage(luigi.Task):

    PackageID = luigi.IntParameter()

    def requires(self):
        return DownloadPackage(self.PackageID)
    
    def output(self):
        return luigi.LocalTarget(
            f"data/analysis_{self.PackageID}.txt"
        )
    
    def run(self):
        with self.input().open("r") as i:
            path = i.read()
            import subprocess
            cmd = ("regexploit-py", f"{path}/**/*.py", "--glob")
            p = subprocess.run(cmd, capture_output=True, text=True)
            with self.output().open("w") as outfile:
                outfile.write(p.stdout)
                outfile.write(p.stderr)

class GenerateReport(luigi.Task):

    def requires(self):
        requiredInputs = []
        for i in range(GlobalParams().NumberPackages):
            requiredInputs.append(AnalysePackage(PackageID=i))
        return requiredInputs
    
    def output(self):
        return luigi.LocalTarget("data/summary.txt")
    
    def run(self):
        with self.output().open("w") as f:
            for input in self.input():
                with input.open("r") as infile:
                    f.write(infile.read())

class GlobalParams(luigi.Config):
    NumberPackages = luigi.IntParameter(default=10)

