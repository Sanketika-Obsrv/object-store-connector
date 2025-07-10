import subprocess
import os

def main():
    # remove dirs recursively
    subprocess.Popen("rm -rf dist", shell=True).wait()

    # Path to the directory containing the JAR files
    jar_dir = os.path.join(os.path.dirname(__file__), '..', 'libs')
    os.makedirs(jar_dir, exist_ok=True)

    subprocess.Popen("""poetry export --without-hashes --format=requirements.txt | awk '{split($0,a,"; "); print a[1]}' > requirements.txt""", shell=True).wait()
    subprocess.Popen("mvn dependency:copy-dependencies -DrepoUrl=http://repo1.maven.org/maven2/ -DexcludeTrans -DoutputDirectory=libs", shell=True).wait()
    subprocess.Popen("poetry build -f sdist", shell=True).wait()
    subprocess.Popen("rm -rf requirements.txt", shell=True).wait()

if __name__ == '__main__':
    main()
