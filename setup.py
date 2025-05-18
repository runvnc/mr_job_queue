from setuptools import setup, find_packages

setup(
    name="mr_job_queue",
    version="2.1.0",
    packages=find_packages(where="src"),
    package_dir={"":"src"},
    package_data={
        "mr_job_queue": [
            "static/js/*.js",
            "static/css/*.css",
            "templates/*.jinja2",
            "inject/*.jinja2",
        ],
    },
    install_requires=[
        "fastapi",
        "aiofiles",
        "python-multipart",  # For file uploads
    ],
    description="A plugin to manage background jobs processed by agents",
    author="MindRoot",
    author_email="info@mindroot.ai",
    url="https://github.com/runvnc/mindroot",
)
