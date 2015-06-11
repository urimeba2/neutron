# Copyright (c) 2015 Mirantis, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from neutron.common import exceptions
from neutron.newapi.hooks import attribute_population
from neutron import quota

from oslo_log import log as logging
from pecan import hooks

LOG = logging.getLogger(__name__)


class QuotaEnforcementHook(hooks.PecanHook):

    priority = 130

    def before(self, state):
        if state.request.method != 'POST':
            return
        items = state.request.resources
        rtype = state.request.resource_type
        deltas = {}
        for item in items:
            tenant_id = item['tenant_id']
            try:
                count = quota.QUOTAS.count(state.request.context, rtype,
                                           state.request.plugin,
                                           attribute_population._plural(rtype),
                                           tenant_id)
                delta = deltas.get(tenant_id, 0) + 1
                kwargs = {rtype: count + delta}
            except exceptions.QuotaResourceUnknown as e:
                # We don't want to quota this resource
                LOG.debug(e)
            else:
                quota.QUOTAS.limit_check(state.request.context, tenant_id,
                                         **kwargs)
