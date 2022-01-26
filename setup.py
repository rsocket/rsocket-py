import os

from setuptools import setup, find_packages


def get_version():
    return os.environ.get('PACKAGE_VERSION', '1.0.0')


setup(
    name='precognize-rsocket',
    version=get_version(),
    description='Python RSocket library (Precognize fork)',
    url='https://github.com/Precognize/rsocket-py',
    author='Gabi Shaar',
    author_email='',
    license='MIT',
    packages=find_packages(exclude=['examples', 'tests', 'docs']),
    zip_safe=True,
    extra_required={
        'rx': {'Rx >= 3.0.0'}
    },
    classifiers=[
        'Development Status :: 1 - Planning',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
