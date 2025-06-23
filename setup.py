# setup.py
import setuptools

setuptools.setup(
    name="scheduler",
    version="0.0.1",
    description="Task scheduler engine",
    author="anikinjura",
    author_email="anikinjura@gmail.com",
    python_requires=">=3.10",
    packages=setuptools.find_packages(
        include=[
            "scheduler_runner*",  # всё внутри scheduler_runner/
            "config",             # сама папка config/
        ]
    ),
    install_requires=[
        # автоматически подтянется из requirements.txt
        # "requests>=2.28.0",
        # "python-telegram-bot>=20.0",
    ],
    entry_points={
        "console_scripts": [
            # позволит запускать через `scheduler-run = scheduler_runner.runner:main`
            "scheduler-run = scheduler_runner.runner:main",
        ],
    },
)
