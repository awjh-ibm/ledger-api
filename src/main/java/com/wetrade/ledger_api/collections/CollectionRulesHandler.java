package com.wetrade.ledger_api.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.parboiled.Node;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

enum Operators {
    OR,
    AND
}

public class CollectionRulesHandler {

    // TODO MAKE THIS ALL STATIC AS SEEMS THAT WOULD BE BETTER BUT AS LIAM

    private List<Node<CollectionRules>> tree;
    private List<String> collections;
    private String input;

    public CollectionRulesHandler(String rules) {
        this.setup(rules, new String[] {});
    }

    public CollectionRulesHandler(String rules, String[] collections) {
        this.setup(rules, collections);
    }

    private void setup(String rules, String[] collections) {
        CollectionRules parser = Parboiled.createParser(CollectionRules.class);

        ParsingResult<CollectionRules> result = new ReportingParseRunner<CollectionRules>(parser.Expression()).run(rules);

        if (!result.matched) {
            throw new RuntimeException("Collection rules invalid");
        }

        this.tree = result.parseTreeRoot.getChildren();
        this.input = rules;
        this.collections = Arrays.asList(collections);
    }

    public Boolean evaluate() {
        return this.evaluate(this.tree.get(0));
    }

    private Boolean evaluate(Node<CollectionRules> node) {
        switch (node.getLabel()) {
            case "AnyOf": return this.AnyOfHandler(node);
            case "AllOf": return this.AllOfHandler(node);
            case "OR": return this.ORHandler(node);
            case "AND": return this.ANDHandler(node);
            case "ComparisonItem": return this.comparisonItemHandler(node);
            case "QuotedString": return this.collectionFound(node);
            default: throw new RuntimeException("Invalid rule label: " + node.getLabel());
        }
    }

    public String[] getEntries() {
        ArrayList<String> arr = new ArrayList<String>();

        this.getEntries(this.tree.get(0), arr);

        return arr.toArray(new String[arr.size()]);
    }

    public void getEntries(Node<CollectionRules> node, ArrayList<String> arr) {
        for (Node<CollectionRules> child : node.getChildren()) {
            if (child.getLabel() == "QuotedString") {
                arr.add(this.parseQuotedString(node));
            } else {
                this.getEntries(child, arr);
            }
        }
    }

    private String parseQuotedString(Node<CollectionRules> node) {
        return this.input.substring(node.getStartIndex() + 1, node.getEndIndex() - 1);
    }

    private Boolean collectionFound(Node<CollectionRules> node) {
        final String collection = this.parseQuotedString(node);
        
        return this.collections.contains(collection);
    }

    private Boolean comparisonItemHandler(Node<CollectionRules> node) {
        return this.evaluate(node.getChildren().get(0));
    }

    private Boolean multiItemHandler(Node<CollectionRules> node, Operators op) {
        final Node<CollectionRules> multiItem = node.getChildren().get(1);

        final List<Node<CollectionRules>> multiItemChildren = multiItem.getChildren();
        final Node<CollectionRules> firstComparisonItem = multiItemChildren.get(0);
        final List<Node<CollectionRules>> otherComparisonItems = multiItemChildren.get(1).getChildren();

        Boolean result = this.evaluate(firstComparisonItem);

        for (Node<CollectionRules> sequence: otherComparisonItems) {
            Node<CollectionRules> comparisonItem = sequence.getChildren().get(2);
            
            switch (op) {
                case OR: result = result || this.evaluate(comparisonItem); break;
                case AND: result = result && this.evaluate(comparisonItem); break;
            }
        }

        return result;
    }

    private Boolean AnyOfHandler(Node<CollectionRules> node) {
        return this.multiItemHandler(node, Operators.OR);
    }

    private Boolean AllOfHandler(Node<CollectionRules> node) {
        return this.multiItemHandler(node, Operators.AND);
    }

    private Boolean ORHandler(Node<CollectionRules> node) {
        final List<Node<CollectionRules>> children = node.getChildren();

        return this.evaluate(children.get(1)) || this.evaluate(children.get(3));
    }

    private Boolean ANDHandler(Node<CollectionRules> node) {
        final List<Node<CollectionRules>> children = node.getChildren();

        return this.evaluate(children.get(1)) && this.evaluate(children.get(3));
    }
}