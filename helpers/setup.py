from setuptools import setup

setup(name='SparkNotebookHelpers',
      version='0.1',
      description='Misc helpers for Spark Python Notebook',
      url='',
      install_requires=['maven-artifact'],
      author='David Taieb',
      author_email='david_taieb@us.ibm.com',
      license='Apache 2.0',
      packages=['packageManager'],
      zip_safe=False)