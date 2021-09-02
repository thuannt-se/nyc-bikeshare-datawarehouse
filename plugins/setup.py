import setuptools

setuptools.setup(
   	name='airflow_custom_operators',
	version="0.0.1",
	author="Thuannt-se",
	author_email="thuannt.se@gmail.com",
	description="A simple custom airflow operators",
	long_description_content_type="text/markdown",
	classifiers=[
	"Programming Language :: Python :: 3",
	"License :: OSI Approved :: MIT License",
	"Operating System :: OS Independent",
	],
	package_dir={"": "package"},
	packages=setuptools.find_packages(where="package"),
	python_requires=">=3.6",
)
