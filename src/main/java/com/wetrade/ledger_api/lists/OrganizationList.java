package com.wetrade.ledger_api.lists;

import com.wetrade.ledger_api.Organization;
import com.wetrade.ledger_api.states.StateList;

import org.hyperledger.fabric.contract.Context;

public class OrganizationList extends StateList<Organization> {
    public OrganizationList(Context ctx, String listName, Class<? extends Organization>[] classes) {
        super(ctx, listName);

        this.use(classes);
    }
}