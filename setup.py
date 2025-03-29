from setuptools import setup, find_packages

setup(
    name="mr_job_queue",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"":"src"},
    install_requires=[
        "aiofiles",
    ],
    package_data={
        "mr_job_queue": [
            "templates/*.jinja2",
            "static/js/*.js",
            "static/css/*.css",
        ],
    },
    author="MindRoot",
    author_email="admin@mindroot.ai",
    description="Simple file-based job queue for running agent tasks asynchronously",
    keywords="mindroot, plugin, job, queue",
    python_requires=">=3.8",
)
