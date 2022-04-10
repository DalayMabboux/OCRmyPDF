#!/usr/bin/env python3
# Copyright (C) 2019 Ian Alexander: https://github.com/ianalexander
# Copyright (C) 2020 James R Barlow: https://github.com/jbarlow83
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pikepdf
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

import ocrmypdf

# pylint: disable=logging-format-interpolation


def getenv_bool(name: str, default: str = 'False'):
    return os.getenv(name, default).lower() in ('true', 'yes', 'y', '1')


INPUT_DIRECTORY = os.getenv('OCR_INPUT_DIRECTORY', '/input')
ON_SUCCESS_DELETE = getenv_bool('OCR_ON_SUCCESS_DELETE')
DESKEW = getenv_bool('OCR_DESKEW')
OCR_JSON_SETTINGS = json.loads(os.getenv('OCR_JSON_SETTINGS', '{}'))
POLL_NEW_FILE_SECONDS = int(os.getenv('OCR_POLL_NEW_FILE_SECONDS', '1'))
USE_POLLING = getenv_bool('OCR_USE_POLLING')
LOGLEVEL = os.getenv('OCR_LOGLEVEL', 'INFO')
PATTERNS = ['*.pdf', '*.PDF']
OCR_LAYER_ADDED_SUFFIX = "_ocrd"

log = logging.getLogger('ocrmypdf-watcher')

def get_output_dir(file_path):
    new_file_name = file_path.stem + OCR_LAYER_ADDED_SUFFIX + file_path.suffix
    return (str(file_path.parent) + '/' + new_file_name)

def wait_for_file_ready(file_path):
    # This loop waits to make sure that the file is completely loaded on
    # disk before attempting to read. Docker sometimes will publish the
    # watchdog event before the file is actually fully on disk, causing
    # pikepdf to fail.

    retries = 5
    while retries:
        try:
            pdf = pikepdf.open(file_path)
        except (FileNotFoundError, pikepdf.PdfError) as e:
            log.info(f"File {file_path} is not ready yet")
            log.debug("Exception was", exc_info=e)
            time.sleep(POLL_NEW_FILE_SECONDS)
            retries -= 1
        else:
            pdf.close()
            return True

    return False


def execute_ocrmypdf(file_path):
    file_path = Path(file_path)
    output_path = get_output_dir(file_path)

    log.info("-" * 20)
    log.info(f'New file: {file_path}. Waiting until fully loaded...')
    if not wait_for_file_ready(file_path):
        log.info(f"Gave up waiting for {file_path} to become ready")
        return
    log.info(f'Attempting to OCRmyPDF to: {output_path}')
    exit_code = ocrmypdf.ocr(
        input_file=file_path,
        output_file=output_path,
        deskew=DESKEW,
        **OCR_JSON_SETTINGS,
    )
    if exit_code == 0 and ON_SUCCESS_DELETE:
        log.info(f'OCR is done. Deleting: {file_path}')
        file_path.unlink()
    else:
        log.info('OCR is done')


class HandleObserverEvent(PatternMatchingEventHandler):
    def on_any_event(self, event):
        if event.event_type in ['created'] and not OCR_LAYER_ADDED_SUFFIX in event.src_path:
            execute_ocrmypdf(event.src_path)


def main():
    ocrmypdf.configure_logging(
        verbosity=(
            ocrmypdf.Verbosity.default
            if LOGLEVEL != 'DEBUG'
            else ocrmypdf.Verbosity.debug
        ),
        manage_root_logger=True,
    )
    log.setLevel(LOGLEVEL)
    log.info(
        f"Starting OCRmyPDF watcher with config:\n"
        f"Input Directory: {INPUT_DIRECTORY}\n"
    )
    log.debug(
        f"INPUT_DIRECTORY: {INPUT_DIRECTORY}\n"
        f"ON_SUCCESS_DELETE: {ON_SUCCESS_DELETE}\n"
        f"DESKEW: {DESKEW}\n"
        f"ARGS: {OCR_JSON_SETTINGS}\n"
        f"POLL_NEW_FILE_SECONDS: {POLL_NEW_FILE_SECONDS}\n"
        f"USE_POLLING: {USE_POLLING}\n"
        f"LOGLEVEL: {LOGLEVEL}"
    )

    if 'input_file' in OCR_JSON_SETTINGS or 'output_file' in OCR_JSON_SETTINGS:
        log.error('OCR_JSON_SETTINGS should not specify input file or output file')
        sys.exit(1)

    handler = HandleObserverEvent(patterns=PATTERNS)
    if USE_POLLING:
        observer = PollingObserver()
    else:
        observer = Observer()
    observer.schedule(handler, INPUT_DIRECTORY, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
