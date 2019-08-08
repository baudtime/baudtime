/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import "github.com/baudtime/baudtime/vars"

var nodePfx, routeInfoPfx, sGrpRoutePfx string

func nodePrefix() string {
	if nodePfx == "" {
		nodePfx = vars.Cfg.NameSpace + "_node_"
	}
	return nodePfx
}

func routeInfoPrefix() string {
	if routeInfoPfx == "" {
		routeInfoPfx = vars.Cfg.NameSpace + "_routeInfo_"
	}
	return routeInfoPfx
}

func sGrpRoutePrefix() string {
	if sGrpRoutePfx == "" {
		sGrpRoutePfx = vars.Cfg.NameSpace + "_sGrpRouteKey_"
	}
	return sGrpRoutePfx
}
