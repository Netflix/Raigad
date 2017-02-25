/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.raigad.backup.exception;

public class NoMasterNodeException extends Exception {

    private static final long serialVersionUID = 1L;

    public NoMasterNodeException(String msg, Throwable th)
    {
        super(msg, th);
    }

    public NoMasterNodeException(String msg)
    {
        super(msg);
    }

    public NoMasterNodeException(Exception ex)
    {
        super(ex);
    }

    public NoMasterNodeException(Throwable th)
    {
        super(th);
    }
}
