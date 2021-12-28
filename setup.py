import os

from setuptools import setup


def get_version():
    return os.environ.get('PACKAGE_VERSION', '1.0.0')


setup(
    name='precognize-rsocket',
    version=get_version(),
    description='Python RSocket library (Precognize fork)',
    url='https://github.com/Precognize/rsocket-pyy',
    author='Gabi Shaar',
    author_email='',
    license='MIT',
    packages=[
        'rsocket',
        'reactivestreams'
    ],
    zip_safe=True,
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
