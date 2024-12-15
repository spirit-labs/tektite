package agent

import (
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	log "github.com/spirit-labs/tektite/logger"
	"sort"
)

func (k *kafkaHandler) HandleDescribeAclsRequest(hdr *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.DescribeAclsRequest, completionFunc func(resp *kafkaprotocol.DescribeAclsResponse) error) error {
	log.Infof("HandleDescribeAclsRequest %v", req)
	log.Infof("resourceType:%d resourceName:%s resourcePatternType:%d principal:%s host:%s operation:%d permissionType:%d ",
		req.ResourceTypeFilter, common.SafeDerefStringPtr(req.ResourceNameFilter),
		req.PatternTypeFilter, common.SafeDerefStringPtr(req.PrincipalFilter),
		common.SafeDerefStringPtr(req.HostFilter), req.Operation,
		req.PermissionType)
	resp := &kafkaprotocol.DescribeAclsResponse{}
	entries, err := k.describeAcls(acls.ResourceType(req.ResourceTypeFilter), common.SafeDerefStringPtr(req.ResourceNameFilter),
		acls.ResourcePatternType(req.PatternTypeFilter), common.SafeDerefStringPtr(req.PrincipalFilter), common.SafeDerefStringPtr(req.HostFilter),
		acls.Operation(req.Operation), acls.Permission(req.PermissionType))
	if err != nil {
		var errCode int16
		if common.IsUnavailableError(err) {
			log.Warnf("failed to create acls: %v", err)
			errCode = kafkaprotocol.ErrorCodeLeaderNotAvailable
		} else {
			log.Errorf("failed to create acls: %v", err)
			errCode = kafkaprotocol.ErrorCodeUnknownServerError
		}
		resp.ErrorCode = errCode
		resp.ErrorMessage = common.StrPtr(err.Error())
	} else {
		// We need to sort the results by [resource type, resource name, pattern type] as the kafka protocol requires
		// response results grouped in this way
		sort.SliceStable(entries, func(i, j int) bool {
			if entries[i].ResourceType != entries[j].ResourceType {
				return entries[i].ResourceType < entries[j].ResourceType
			}
			if entries[i].ResourceName != entries[j].ResourceName {
				return entries[i].ResourceName < entries[j].ResourceName
			}
			return entries[i].ResourcePatternType < entries[j].ResourcePatternType
		})
		var resources []kafkaprotocol.DescribeAclsResponseDescribeAclsResource
		var currResource *kafkaprotocol.DescribeAclsResponseDescribeAclsResource
		for _, entry := range entries {
			if currResource == nil || (currResource.ResourceType != int8(entry.ResourceType) ||
				common.SafeDerefStringPtr(currResource.ResourceName) != entry.ResourceName || currResource.PatternType != int8(entry.ResourcePatternType)) {
				newResource := &kafkaprotocol.DescribeAclsResponseDescribeAclsResource{
					ResourceType: int8(entry.ResourceType),
					ResourceName: common.StrPtr(entry.ResourceName),
					PatternType:  int8(entry.ResourcePatternType),
				}
				if currResource != nil {
					resources = append(resources, *currResource)
				}
				currResource = newResource
			}
			currResource.Acls = append(currResource.Acls, kafkaprotocol.DescribeAclsResponseAclDescription{
				Principal:      common.StrPtr(entry.Principal),
				Host:           common.StrPtr(entry.Host),
				Operation:      int8(entry.Operation),
				PermissionType: int8(entry.Permission),
			})
		}
		if currResource != nil {
			resources = append(resources, *currResource)
		}
		resp.Resources = resources
	}
	return completionFunc(resp)
}

func (k *kafkaHandler) describeAcls(resourceType acls.ResourceType, resourceNameFilter string, patternTypeFilter acls.ResourcePatternType,
	principal string, host string, operation acls.Operation, permission acls.Permission) ([]acls.AclEntry, error) {
	cl, err := k.agent.controlClientCache.GetClient()
	if err != nil {
		return nil, err
	}
	return cl.ListAcls(resourceType, resourceNameFilter, patternTypeFilter, principal, host, operation, permission)
}

func (k *kafkaHandler) HandleCreateAclsRequest(hdr *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.CreateAclsRequest, completionFunc func(resp *kafkaprotocol.CreateAclsResponse) error) error {
	log.Infof("in HandleCreateAclsRequest, num entries %d %v", len(req.Creations), req)
	aclEntries := make([]acls.AclEntry, 0, len(req.Creations))
	for _, creation := range req.Creations {
		log.Infof("resourceType:%d resourceName:%s resourcePatternType:%d principal:%s host:%s operation:%d permissionType:%d ",
			creation.ResourceType, common.SafeDerefStringPtr(creation.ResourceName),
			creation.ResourcePatternType, common.SafeDerefStringPtr(creation.Principal),
			common.SafeDerefStringPtr(creation.Host), creation.Operation,
			creation.PermissionType)
		entry := acls.AclEntry{
			Principal:           common.SafeDerefStringPtr(creation.Principal),
			Permission:          acls.Permission(creation.PermissionType),
			Operation:           acls.Operation(creation.Operation),
			Host:                common.SafeDerefStringPtr(creation.Host),
			ResourceType:        acls.ResourceType(creation.ResourceType),
			ResourceName:        common.SafeDerefStringPtr(creation.ResourceName),
			ResourcePatternType: acls.ResourcePatternType(creation.ResourcePatternType),
		}
		aclEntries = append(aclEntries, entry)
	}
	resp := kafkaprotocol.CreateAclsResponse{
		Results: make([]kafkaprotocol.CreateAclsResponseAclCreationResult, len(req.Creations)),
	}
	log.Infof("sending create acls request: %v", aclEntries)
	if err := k.createAcls(aclEntries); err != nil {
		var errCode int16
		if common.IsUnavailableError(err) {
			log.Warnf("failed to create acls: %v", err)
			errCode = kafkaprotocol.ErrorCodeLeaderNotAvailable
		} else {
			log.Errorf("failed to create acls: %v", err)
			errCode = kafkaprotocol.ErrorCodeUnknownServerError
		}
		for i := 0; i < len(resp.Results); i++ {
			resp.Results[i].ErrorCode = errCode
			resp.Results[i].ErrorMessage = common.StrPtr(err.Error())
		}
	}
	return completionFunc(&resp)
}

func (k *kafkaHandler) createAcls(aclEntries []acls.AclEntry) error {
	cl, err := k.agent.controlClientCache.GetClient()
	if err != nil {
		return err
	}
	return cl.CreateAcls(aclEntries)
}

func (k *kafkaHandler) HandleDeleteAclsRequest(hdr *kafkaprotocol.RequestHeader,
	req *kafkaprotocol.DeleteAclsRequest, completionFunc func(resp *kafkaprotocol.DeleteAclsResponse) error) error {
	resp := kafkaprotocol.DeleteAclsResponse{
		FilterResults: make([]kafkaprotocol.DeleteAclsResponseDeleteAclsFilterResult, len(req.Filters)),
	}
	cl, err := k.agent.controlClientCache.GetClient()
	if err != nil {
		errCode := errorCodeForError(err)
		for i := 0; i < len(resp.FilterResults); i++ {
			resp.FilterResults[i].ErrorCode = errCode
			resp.FilterResults[i].ErrorMessage = common.StrPtr(err.Error())
		}
	} else {
		for i, filter := range req.Filters {
			log.Infof("delete acl: resourceType:%d resourceName:%s resourcePatternType:%d principal:%s host:%s operation:%d permissionType:%d ",
				filter.ResourceTypeFilter, common.SafeDerefStringPtr(filter.ResourceNameFilter),
				filter.PatternTypeFilter, common.SafeDerefStringPtr(filter.PrincipalFilter),
				common.SafeDerefStringPtr(filter.HostFilter), filter.Operation,
				filter.PermissionType)
			if err = cl.DeleteAcls(acls.ResourceType(filter.ResourceTypeFilter), common.SafeDerefStringPtr(filter.ResourceNameFilter),
				acls.ResourcePatternType(filter.PatternTypeFilter), common.SafeDerefStringPtr(filter.PrincipalFilter), common.SafeDerefStringPtr(filter.HostFilter),
				acls.Operation(filter.Operation), acls.Permission(filter.PermissionType)); err != nil {
				errCode := errorCodeForError(err)
				resp.FilterResults[i].ErrorCode = errCode
				resp.FilterResults[i].ErrorMessage = common.StrPtr(err.Error())
			}
		}
	}
	return completionFunc(&resp)
}

func errorCodeForError(err error) int16 {
	var errCode int16
	if common.IsUnavailableError(err) {
		log.Warnf("failed to create acls: %v", err)
		errCode = kafkaprotocol.ErrorCodeLeaderNotAvailable
	} else {
		log.Errorf("failed to create acls: %v", err)
		errCode = kafkaprotocol.ErrorCodeUnknownServerError
	}
	return errCode
}
