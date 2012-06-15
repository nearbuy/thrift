/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <ruby.h>
#include <constants.h>
#include "macros.h"

ID rbuf_ivar_id;
ID framed_index_ivar_id;
ID framed_transport_ivar_id;
ID read_ivar_id;

ID read_frame_method_id;
ID read_method_id;
ID framed_read_byte_method_id;
ID framed_slice_method_id;

#define GET_RBUF(self) rb_ivar_get(self, rbuf_ivar_id)

VALUE rb_thrift_framed_read(VALUE self, VALUE length_value);
VALUE rb_thrift_framed_read_byte(VALUE self);
VALUE rb_thrift_framed_read_into_buffer(VALUE self, VALUE buffer_value, VALUE size_value);

VALUE rb_thrift_framed_read(VALUE self, VALUE length_value) {
  int length = FIX2INT(length_value);
  int read = rb_ivar_get(self, read_ivar_id) == Qtrue;
  int index = FIX2INT(rb_ivar_get(self, framed_index_ivar_id));
  VALUE rbuf = GET_RBUF(self);

  if (!read) {
    return rb_funcall(rb_ivar_get(self, framed_transport_ivar_id), read_method_id, 1, length_value);
  }

  if (length <= 0) {
    return rb_str_new("", 0);
  }

  if (index >= RSTRING_LEN(rbuf)) {
    rb_funcall(self, read_frame_method_id, 0);
    index = FIX2INT(rb_ivar_get(self, framed_index_ivar_id));
    rbuf = GET_RBUF(self);
  }

  index += length;
  rb_ivar_set(self, framed_index_ivar_id, INT2FIX(index));

  return rb_funcall(rbuf, framed_slice_method_id, 2, INT2FIX(index-length), length_value);
}

VALUE rb_thrift_framed_read_byte(VALUE self) {
  int read = rb_ivar_get(self, read_ivar_id) == Qtrue;
  int index = FIX2INT(rb_ivar_get(self, framed_index_ivar_id));
  VALUE rbuf = GET_RBUF(self);

  if (!read) {
    return rb_funcall(rb_ivar_get(self, framed_transport_ivar_id), framed_read_byte_method_id, 0);
  }

  if (index >= RSTRING_LEN(rbuf)) {
    rb_funcall(self, read_frame_method_id, 0);
    index = FIX2INT(rb_ivar_get(self, framed_index_ivar_id));
    rbuf = GET_RBUF(self);
  }

  char byte = RSTRING_PTR(rbuf)[index++];
  rb_ivar_set(self, framed_index_ivar_id, INT2FIX(index));

  int result = (int)byte;
  return INT2FIX(result);
}

VALUE rb_thrift_framed_read_into_buffer(VALUE self, VALUE buffer_value, VALUE size_value) {
  int i = 0;
  int size = FIX2INT(size_value);
  int index = FIX2INT(rb_ivar_get(self, framed_index_ivar_id));
  VALUE rbuf = GET_RBUF(self);

  while (i < size) {
    if (index >= RSTRING_LEN(rbuf)) {
      rb_funcall(self, read_frame_method_id, 0);
      index = FIX2INT(rb_ivar_get(self, framed_index_ivar_id));
      rbuf = GET_RBUF(self);
    }

    if (i >= RSTRING_LEN(buffer_value)) {
      rb_raise(rb_eIndexError, "index %d out of string", i);
    }
    ((char*)RSTRING_PTR(buffer_value))[i++] = RSTRING_PTR(rbuf)[index++];
  }

  rb_ivar_set(self, framed_index_ivar_id, INT2FIX(index));
  return INT2FIX(i);
}

void Init_framed() {
  /* VALUE thrift_framed_class = rb_const_get(thrift_module, rb_intern("FramedTransport")); */
  /* rb_define_method(thrift_framed_class, "read", rb_thrift_framed_read, 1); */
  /* rb_define_method(thrift_framed_class, "read_byte", rb_thrift_framed_read_byte, 0); */
  /* rb_define_method(thrift_framed_class, "read_into_buffer", rb_thrift_framed_read_into_buffer, 2); */

  rbuf_ivar_id = rb_intern("@rbuf");
  framed_index_ivar_id = rb_intern("@index");
  framed_transport_ivar_id = rb_intern("@transport");
  read_ivar_id = rb_intern("@read");

  read_method_id = rb_intern("read");
  read_frame_method_id = rb_intern("read_frame");
  framed_read_byte_method_id = rb_intern("read_byte");
  framed_slice_method_id = rb_intern("slice");
}
