package nz.co.gregs.dbvolution.generation.compiling.deprecated.datarepo;

import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.datatypes.spatial2D.*;
import nz.co.gregs.dbvolution.annotations.*;

@DBTableName("ANTAGONIST") 
public class Antagonist extends DBRow {

    public static final long serialVersionUID = 1L;

    @DBColumn("ANTAGONISTID")
    @DBPrimaryKey
    @DBAutoIncrement
    public DBInteger antagonistid = new DBInteger();

    @DBColumn("NAME")
    public DBString name = new DBString();

    @DBColumn("NPC")
    public DBBoolean npc = new DBBoolean();

}

