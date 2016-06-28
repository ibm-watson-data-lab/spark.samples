from setuptools import setup

setup(name='pixiedust',
      version='0.1',
      description='Misc helpers for Spark Python Notebook',
      url='https://github.com/ibm-cds-labs/spark.samples/tree/master/pixiedust',
      install_requires=['maven-artifact'],
      author='David Taieb',
      author_email='david_taieb@us.ibm.com',
      license='Apache 2.0',
      packages=['packageManager','display'],
      zip_safe=False)