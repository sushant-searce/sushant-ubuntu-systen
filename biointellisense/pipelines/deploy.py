#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys

from unittest import TestCase
from typing import List, Dict


class TestDeployPipelines(TestCase):
    def test_protected_environments_are_not_loaded(self):
        self.assertRaises(RuntimeError, main, '_test', [])

    def test_loader_reads_file(self):
        options = Configuration('_test').options()
        self.assertGreater(len(options), 0)

    def test_excepts_if_file_is_not_found(self):
        self.assertRaises(FileNotFoundError, Configuration, 'invalid_environment')

    def test_common_options_are_loaded(self):
        options = Configuration('_test').options()
        self.assertTrue('option1' in options)

    def test_ignore_unknown_fields(self):
        options = Configuration('_test').options()
        self.assertFalse('field' in options)

    def test_loads_pipeline_options(self):
        options = Configuration('_test').options('TestPipeline')
        self.assertTrue('pipelineOption' in options)

    def test_pipeline_options_overwrite_common_options(self):
        options = Configuration('_test').options('TestPipeline')
        self.assertEqual(options['overwrittenOption'], 'overwritten_value')

    def test_only_common_options_are_returned_if_pipeline_is_empty(self):
        options = Configuration('_test').options('EmptyPipeline')
        self.assertEqual(options, Configuration('_test').options())

    def test_fails_if_there_is_no_such_pipeline(self):
        self.assertRaises(KeyError, Configuration('_test').options, 'UnconfiguredPipeline')

    def test_pipeline_option_in_null_removes_option_from_common_options(self):
        options = Configuration('_test').options('TestPipeline')
        self.assertFalse('removedOption' in options)

    def test_list_all_pipelines(self):
        pipelines = Configuration('_test').pipelines()
        self.assertEqual(len(pipelines), 2)
        self.assertTrue('TestPipeline' in pipelines)
        self.assertTrue('EmptyPipeline' in pipelines)

    def test_append_options(self):
        additional_opts = [' newOption= newValue', 'overwrittenOption =externalValue  ', 'log4j.rootLogger=DEBUG']
        options = append_options(additional_opts, Configuration('_test').options('TestPipeline'))
        self.assertEqual(options['newOption'], 'newValue')
        self.assertEqual(options['overwrittenOption'], 'externalValue')
        self.assertEqual(options['log4j.rootLogger'], 'DEBUG')

    def test_remove_options(self):
        additional_opts = ['overwrittenOption= ']
        options = append_options(additional_opts, Configuration('_test').options('TestPipeline'))
        self.assertFalse('overwrittenOption' in options)

    def test_append_fails_if_ill_formed_pair_is_provided(self):
        additional_opts = [' newOption= newValue', 'overwrittenOption =externalValue  ', 'illformed']
        self.assertRaises(RuntimeError, append_options, additional_opts, Configuration('_test').options('TestPipeline'))

    def test_returns_default_class_name_if_not_specified(self):
        class_name = Configuration('_test').class_name('EmptyPipeline')
        self.assertEqual(class_name, 'com.striiv.dataflow.EmptyPipeline')

    def test_returns_configured_class_name(self):
        class_name = Configuration('_test').class_name('TestPipeline')
        self.assertEqual(class_name, 'com.someproject.pipelines.TestPipeline')

    def test_options_are_mapped_into_mvn_arguments(self):
        options = Configuration('_test').options('TestPipeline')
        mvn_args = pipeline_args(options)
        for key, value in options.items():
            self.assertTrue(f'--{key}={value}' in mvn_args)


class Configuration:
    def __init__(self, environment: str):
        with open(f'environments/{environment}.json'.lower()) as json_file:
            self._data = json.load(json_file)

    def options(self, pipeline=None) -> Dict:
        opts = {}
        if 'common' in self._data:
            opts = {**self._data['common']}

        if 'pipelines' in self._data and pipeline is not None:
            opts = {**opts, **(self._data['pipelines'][pipeline].get('args', {}))}

        return {k: v for k, v in opts.items() if v is not None}

    def pipelines(self) -> List[str]:
        if 'pipelines' not in self._data:
            return []
        return self._data['pipelines'].keys()

    def class_name(self, pipeline):
        return self._data['pipelines'][pipeline].get('class', f'com.striiv.dataflow.{pipeline}')


def append_options(external_opts: List[str], original_opts: Dict) -> Dict:
    valid_kv_pair = re.compile('(?i)\s*\w[a-z][\w.-]+\w\s*=.*')
    if external_opts is None:
        return original_opts

    options = original_opts
    for item in external_opts:
        if not valid_kv_pair.match(item):
            raise RuntimeError(f'Invalid argument: {item}')
        key, value = item.split('=')

        options[key.strip()] = value.strip()
        if not value.strip():
            options.pop(key.strip())

    return options


def pipeline_args(options: Dict):
    mvn_args = ''
    for key, value in options.items():
        mvn_args += f' --{key}={value}'
    return mvn_args.strip()


def available_workflows():
    return [func for func in dir(Workflows) if callable(getattr(Workflows, func)) and not func.startswith("_")]


class Workflows:
    """
    Container class for different maven workflows supported by the script
    All methods should have the signature (pipeline, class_name, options, pretend)
    and all of them should be static.
    """
    @staticmethod
    def execute(_, class_name, options, pretend):
        mvn_args = pipeline_args(options)
        command = f'mvn compile exec:java -Dexec.mainClass={class_name} -Dexec.args="{mvn_args}"'
        print(command)
        if not pretend:
            return_code = os.system(command)
            if return_code != 0:
                raise Exception(f'mvn compile command failed with return code {return_code}')

    @staticmethod
    def package(pipeline, class_name, options, pretend):
        command = f'mvn package -Dpipeline.name={pipeline} -Dpipeline.mainClass={class_name}'
        print(command)
        print(f'Runtime options: {options}')
        if not pretend:
            return_code = os.system(command)
            if return_code != 0:
                raise Exception(f'mvn package command failed with return code {return_code}')
            with open(f'target/{pipeline}-OPTIONS.json', 'w') as options_file:
                json.dump(options, options_file)


def main(environment: str, pipelines: List[str], workflow: str = 'execute', pretend: bool = True, opt_args: List[str] = None):
    # Environments prepended with a _ are considered private and could not be loaded (testing purposes)
    if environment[0] == '_':
        raise RuntimeError("Attempting to run private environment")

    config = Configuration(environment)
    for pipeline in pipelines:
        options = append_options(opt_args, config.options(pipeline))
        class_name = config.class_name(pipeline)
        getattr(Workflows, workflow)(pipeline, class_name, options, pretend)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy apache beam pipelines.')
    parser.add_argument('--env', action='store', required=True,
                        help='Environment where the pipelines will be deployed')
    parser.add_argument('--list', action='store_true',
                        help='List all available pipelines for the environment')
    parser.add_argument('pipelines', nargs='*',
                        help='Pipelines to be deployed')
    parser.add_argument('--show', action='store_true',
                        help='Only prints the commands')
    parser.add_argument('--args', action='append',
                        help='Allows the override of options. They shall be provided as key=value pairs.')
    parser.add_argument('--workflow', choices=available_workflows(), default='execute',
                        help='Selects the maven workflow to be executed (default: execute)')
    args = parser.parse_args()

    if args.list:
        [print(pipeline) for pipeline in Configuration(args.env).pipelines()]

    try:
        main(args.env, args.pipelines, args.workflow, args.show, args.args)
    except Exception as e:
        print(f'Error: {e}')
        sys.exit(-1)