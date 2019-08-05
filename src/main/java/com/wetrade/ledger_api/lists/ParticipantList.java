package com.wetrade.ledger_api.lists;

import com.wetrade.ledger_api.Participant;
import com.wetrade.ledger_api.states.StateList;

import org.hyperledger.fabric.contract.Context;

public class ParticipantList extends StateList<Participant> {
    public ParticipantList(Context ctx, String listName, Class<Participant>[] classes) {
        super(ctx, listName);

        this.use(classes);
    }
}