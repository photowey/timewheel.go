// Copyright © 2026-present The Timewheel.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

// PanicHandler observes panics recovered at worker boundaries.
type PanicHandler interface {
	HandlePanic(recovered any)
}

// PanicHandlerFunc adapts a named function to PanicHandler.
type PanicHandlerFunc func(recovered any)

var _ PanicHandler = PanicHandlerFunc(nil)

// HandlePanic invokes fn with the recovered value.
func (fn PanicHandlerFunc) HandlePanic(recovered any) {
	fn(recovered)
}
