import os
import yaml
import queue
import logging
import argparse
import subprocess

import kubernetes
import kubernetes.client

from utils.threading import SupervisedThread, SupervisedThreadGroup
from utils.kubernetes.watch import KubeWatcher, WatchEventType
from utils.signal import install_shutdown_signal_handlers

log = logging.getLogger(__name__)


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level='INFO')
    install_shutdown_signal_handlers()

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('repo_path')
    arg_parser.add_argument('--api-url')
    arg_parser.add_argument('--git-email')
    arg_parser.add_argument('--git-username')
    arg_parser.add_argument('--in-cluster', action=argparse.BooleanOptionalAction)
    args = arg_parser.parse_args()

    if args.api_url:
        configuration = kubernetes.client.Configuration()
        configuration.host = args.api_url
        kubernetes.client.Configuration.set_default(configuration)
    elif args.in_cluster:
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config()

    init_repo(args)

    q = queue.Queue()
    threads = SupervisedThreadGroup()
    threads.add_thread(WatcherThread(q))
    threads.add_thread(HandlerThread(q, args.repo_path))
    threads.start_all()
    threads.wait_any()


def init_repo(args):
    if not os.path.exists(os.path.join(args.repo_path, '.git')):
        os.makedirs(args.repo_path, exist_ok=True)
        run_git(args.repo_path, 'init')

    run_git(args.repo_path, 'config', 'user.email', args.git_email)
    run_git(args.repo_path, 'config', 'user.name', args.git_username)


class WatcherThread(SupervisedThread):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run_supervised(self):
        v1 = kubernetes.client.CoreV1Api()
        for event_type, pod in KubeWatcher(v1.list_pod_for_all_namespaces):
            self.queue.put((event_type, pod))


class HandlerThread(SupervisedThread):
    def __init__(self, queue, repo_path):
        super().__init__()
        self.queue = queue
        self.repo_path = repo_path
        self.v1 = kubernetes.client.CoreV1Api()

    def run_supervised(self):
        while True:
            event_type, pod = self.queue.get()
            try:
                self.handle(event_type, pod)
            except Exception:
                log.exception('Failed to handle %s on pod %s/%s',
                              event_type.name, pod.metadata.namespace, pod.metadata.name)

    def handle(self, event_type, pod):
        if event_type == WatchEventType.DONE_INITIAL:
            log.info('Done initial dump')
            return

        log.info('%s %s/%s', event_type.name, pod.metadata.namespace, pod.metadata.name)

        file_path = os.path.join(self.repo_path, pod.metadata.namespace, pod.metadata.name + '.yaml')
        if event_type in (WatchEventType.ADDED, WatchEventType.MODIFIED):
            self.handle_update(file_path, pod)
        elif event_type == WatchEventType.DELETED:
            self.handle_delete(file_path, pod)

        run_git(self.repo_path, 'add', '-A')
        try:
            run_git(self.repo_path, 'commit', '-m', f'{event_type.name} {pod.metadata.namespace}/{pod.metadata.name}')
        except subprocess.CalledProcessError as err:
            if err.returncode != 1:
                raise err

    def handle_update(self, file_path, pod):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        manifest = self.v1.api_client.sanitize_for_serialization(pod)
        clean_manifest(manifest)
        with open(file_path, 'w') as f:
            yaml.dump(manifest, f)

    def handle_delete(self, file_path, pod):
        os.remove(file_path)


def run_git(repo_path, *args):
    cmd = ['git', '-C', repo_path, *args]
    log.info('Running %s', cmd)
    subprocess.run(cmd, check=True)


def clean_manifest(manifest):
    try:
        del manifest['metadata']['managedFields']
    except KeyError:
        pass


if __name__ == '__main__':
    main()
