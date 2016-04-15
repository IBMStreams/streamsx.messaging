/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.messaging.common;

import java.util.HashMap;
import java.util.Map;

import com.ibm.streams.operator.AbstractOperator;

public class DataGovernanceUtil {

	public static void registerForDataGovernance(AbstractOperator operator, String assetName, String assetType, String parentAssetName, String parentAssetType, boolean isInput, String operatorType) {
		Map<String, String> properties = new HashMap<String, String>();		
		if(isInput) {
			properties.put(IGovernanceConstants.TAG_REGISTER_TYPE, IGovernanceConstants.TAG_REGISTER_TYPE_INPUT);
			properties.put(IGovernanceConstants.PROPERTY_INPUT_OPERATOR_TYPE, operatorType);
		} else {
			properties.put(IGovernanceConstants.TAG_REGISTER_TYPE, IGovernanceConstants.TAG_REGISTER_TYPE_OUTPUT);
			properties.put(IGovernanceConstants.PROPERTY_OUTPUT_OPERATOR_TYPE, operatorType);
		}
		properties.put(IGovernanceConstants.PROPERTY_SRC_NAME, assetName);
		properties.put(IGovernanceConstants.PROPERTY_SRC_TYPE, IGovernanceConstants.ASSET_STREAMS_PREFIX + assetType);
		if(parentAssetName != null) {
			properties.put(IGovernanceConstants.PROPERTY_SRC_PARENT_PREFIX, IGovernanceConstants.PROPERTY_PARENT_PREFIX);
			properties.put(IGovernanceConstants.PROPERTY_PARENT_PREFIX + IGovernanceConstants.PROPERTY_SRC_NAME, parentAssetName);
			properties.put(IGovernanceConstants.PROPERTY_PARENT_PREFIX + IGovernanceConstants.PROPERTY_SRC_TYPE, IGovernanceConstants.ASSET_STREAMS_PREFIX + parentAssetType);		
			properties.put(IGovernanceConstants.PROPERTY_PARENT_PREFIX + IGovernanceConstants.PROPERTY_PARENT_TYPE, "$" + parentAssetType);
		}
		
		operator.setTagData(IGovernanceConstants.TAG_OPERATOR_IGC, properties);
	}
}
