# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

import io
from typing import Callable, Any  # noqa
import struct
import time

from pgsqltoolsservice.converters.bytes_converter import get_bytes_converter
from pgsqltoolsservice.query.data_storage.service_buffer import ServiceBufferFileStream
from pgsqltoolsservice.query.data_storage import StorageDataReader


class ServiceBufferFileStreamWriter(ServiceBufferFileStream):
    """ Writer for service buffer formatted file streams """

    WRITER_STREAM_NONE_ERROR = "Stream argument is None"
    WRITER_STREAM_NOT_SUPPORT_WRITING_ERROR = "Stream argument doesn't support writing"
    WRITER_DATA_WRITE_ERROR = "Data write error"
    CONVERTER_DATA_TYPE_NOT_EXIST_ERROR = "Convert to bytes not supported"

    def __init__(self, stream: io.BufferedWriter) -> None:

        if stream is None:
            raise ValueError(ServiceBufferFileStreamWriter.WRITER_STREAM_NONE_ERROR)

        if not stream.writable():
            raise ValueError(ServiceBufferFileStreamWriter.WRITER_STREAM_NOT_SUPPORT_WRITING_ERROR)

        ServiceBufferFileStream.__init__(self, stream)

    # def _write_null(self):
    #     val_byte_array = bytearray([])
    #     return self._write_to_file(self._file_stream, val_byte_array)

    # def _write_to_file(self, stream, byte_array):
    #     try:
    #         written_byte_number = stream.write(byte_array)
    #     except Exception as exc:
    #         raise IOError(ServiceBufferFileStreamWriter.WRITER_DATA_WRITE_ERROR) from exc

    #     return written_byte_number

    def write_row(self, reader: StorageDataReader, duration1, duration2, duration3, duration4, duration5, duration6, duration7, duration8, duration9):
        """   Write a row to a file   """
        # Define a object list to store multiple columns in a row
        start1 = time.time() #-----------
        tmp_columns_info = reader._columns_info
        len_columns_info = len(tmp_columns_info)
        #values = []
        cur_value = 0
        duration1 += time.time() - start1 #-----------

        # Loop over all the columns and write the values to the temp file
        row_bytes = 0

        start2 = time.time() #-----------
        range_func = range(len_columns_info)
        duration2 += time.time() - start2 #-----------
        for index in range_func:

            start3 = time.time() #-----------
            column = tmp_columns_info[index]
            duration3 += time.time() - start3 #-----------
            
            start4 = time.time() #-----------
            #values.append(reader.get_value(index))
            #values.append(reader._current_row[index])
            cur_value = reader._current_row[index]            
            duration4 += time.time() - start4 #-----------

            start5 = time.time() #-----------
            type_value = column.data_type
            duration5 += time.time() - start5 #-----------

            # Write the object into the temp file
            if reader.is_none(index):
                #row_bytes += self._write_null()
                row_bytes += self._file_stream.write(bytearray([]))
            else:
                start6 = time.time() #-----------
                bytes_converter: Callable[[str], bytearray] = get_bytes_converter(type_value)
                duration6 += time.time() - start6 #-----------
                
                start7 = time.time() #-----------
                #value_to_write = bytes_converter(values[index])
                value_to_write = bytes_converter(cur_value)
                duration7 += time.time() - start7 #-----------

                start8 = time.time() #-----------
                bytes_length_to_write = len(value_to_write)
                duration8 += time.time() - start8 #-----------
                
                start9 = time.time() #-----------
                # row_bytes += self._write_to_file(self._file_stream, bytearray(struct.pack("i", bytes_length_to_write)))
                # row_bytes += self._write_to_file(self._file_stream, value_to_write)
                try:
                    row_bytes += self._file_stream.write(bytearray(struct.pack("i", bytes_length_to_write)))
                    row_bytes += self._file_stream.write(value_to_write)                
                except Exception as exc:
                    raise IOError(ServiceBufferFileStreamWriter.WRITER_DATA_WRITE_ERROR) from exc
                duration9 += time.time() - start9 #-----------

        return [row_bytes, duration1, duration2, duration3, duration4, duration5, duration6, duration7, duration8, duration9]

    def seek(self, offset):
        self._file_stream.seek(offset, io.SEEK_SET)
