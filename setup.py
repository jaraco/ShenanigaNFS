import setuptools

setuptools.setup(
    name="ShenanigaNFS",
    version="0.0.1",
    author="Jordan Milne",
    author_email="JordanMilne@users.noreply.github.com",
    description="Library for making somewhat conformant NFS and SunRPC clients and servers",
    long_description_content_type="text/markdown",
    url="https://github.com/JordanMilne/ShenanigaNFS",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "ply~=3.11"
    ],
    python_requires='>=3.7',
)
