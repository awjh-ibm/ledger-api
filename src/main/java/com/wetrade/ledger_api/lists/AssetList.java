package com.wetrade.ledger_api.lists;

import com.wetrade.ledger_api.Asset;
import com.wetrade.ledger_api.states.StateList;

import org.hyperledger.fabric.contract.Context;

public class AssetList extends StateList<Asset> {
    public AssetList(Context ctx, String listName, Class<Asset>[] classes) {
        super(ctx, listName);

        this.use(classes);
    }
}