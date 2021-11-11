from setuptools import setup


def get_requirements():
    with open('requirements.txt') as f:
        requirements = [p.strip().split('=')[0] for p in f.readlines() if p[0] != '-']
    return requirements


setup(
    name='rhizopus',
    version='0.0.9',
    author='Julian Wergieluk',
    author_email='julian@wergieluk.com',
    packages=[
        'rhizopus',
    ],
    url='https://github.com/jwergieluk/rhizopus',
    install_requires=[],
    description='Trading simulation framework',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)
