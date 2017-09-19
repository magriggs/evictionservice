package com.gridgain.proserv.evictionservice.model;

public class Declaration {
    public Declaration(int declarationId, int rowId, double value) {
        this.declarationId = declarationId;
        this.rowId = rowId;
        this.value = value;
    }

    public int declarationId;
    public int rowId;
    public double value;
}
