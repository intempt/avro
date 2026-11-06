/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.specific;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.runtime.resource.loader.FileResourceLoader;

/**
 * Keeps templates written for Velocity 1.7 working on Velocity 2. Velocity 2
 * dropped the {@code $velocityCount} and {@code $velocityHasNext} loop
 * variables in favour of {@code $foreach.count} and {@code $foreach.hasNext};
 * the resource loaders below rewrite the old names while a template is read,
 * so custom templates (templateDirectory) need no change.
 */
public final class Velocity17Templates {

  private static final Pattern COUNT =
      Pattern.compile("(\\$!?\\{?)velocityCount\\b");
  private static final Pattern HAS_NEXT =
      Pattern.compile("(\\$!?\\{?)velocityHasNext\\b");

  private Velocity17Templates() {
  }

  /** Rewrites the Velocity 1.7 loop variables of a template source. */
  static String rewrite(String template) {
    String out = COUNT.matcher(template).replaceAll("$1foreach.count");
    return HAS_NEXT.matcher(out).replaceAll("$1foreach.hasNext");
  }

  static Reader rewrite(Reader reader) {
    if (reader == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    char[] buf = new char[8192];
    try {
      try {
        int n;
        while ((n = reader.read(buf)) != -1) {
          sb.append(buf, 0, n);
        }
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new ResourceNotFoundException("Cannot read template", e);
    }
    return new StringReader(rewrite(sb.toString()));
  }

  /** Classpath loader that accepts Velocity 1.7 loop variables. */
  public static class ClasspathLoader extends ClasspathResourceLoader {
    @Override
    public Reader getResourceReader(String name, String encoding) {
      return rewrite(super.getResourceReader(name, encoding));
    }
  }

  /** File loader that accepts Velocity 1.7 loop variables. */
  public static class FileLoader extends FileResourceLoader {
    @Override
    public Reader getResourceReader(String name, String encoding) {
      return rewrite(super.getResourceReader(name, encoding));
    }
  }
}
