package com.subhadip.datory.preprocessing.model;

import java.io.Serializable;
import java.util.HashMap;

public class LayoutModel implements Serializable {

    private static final long serialVersionUID = -800301365650579723L;

    private String layoutCd;

    private Delimited delimited;
    private FixedWidth fixedWidth;
    private Shared shared;

    LayoutModel() {
        this.delimited = new Delimited();
        this.fixedWidth = new FixedWidth();
        this.shared = new Shared();
    }

    public Shared getShared() {
        return shared;
    }

    public void setShared(Shared shared) {
        this.shared = shared;
    }

    public Delimited getDelimited() {
        return delimited;
    }

    public void setDelimited(Delimited delimited) {
        this.delimited = delimited;
    }

    public String getLayoutCd() {
        return layoutCd;
    }

    public void setLayoutCd(String layoutCd) {
        this.layoutCd = layoutCd;
    }

    public FixedWidth getFixedWidth() {
        return fixedWidth;
    }

    public void setFixedWidth(FixedWidth fixedWidth) {
        this.fixedWidth = fixedWidth;
    }

    public class Shared {

        private int headerNum;
        private int footerNum;


        public int getHeaderNum() {
            return headerNum;
        }

        public void setHeaderNum(int headerNum) {
            this.headerNum = headerNum;
        }

        public int getFooterNum() {
            return footerNum;
        }

        public void setFooterNum(int footerNum) {
            this.footerNum = footerNum;
        }
    }



    public class Delimited {


        private int numberOfColumns;
        private String colDelimiter;
        private BoundaryDefinition boundaryDefinition;

        Delimited() {
            this.boundaryDefinition = new Delimited.BoundaryDefinition();
        }

        public int getNumberOfColumns() {
            return numberOfColumns;
        }

        public void setNumberOfColumns(int numberOfColumns) {
            this.numberOfColumns = numberOfColumns;
        }

        public String getColDelimiter() {
            return colDelimiter;
        }

        public void setColDelimiter(String colDelimiter) {
            this.colDelimiter = colDelimiter;
        }


        public Delimited.BoundaryDefinition getBoundaryDefinition() {
            return boundaryDefinition;
        }

        public void setBoundaryDefinition(Delimited.BoundaryDefinition boundaryDefinition) {
            this.boundaryDefinition = boundaryDefinition;
        }

        public class BoundaryDefinition {

            private String startChar;
            private String lineEndingChar;
            private String endChar;

            public String getStartChar() {
                return startChar;
            }

            public void setStartChar(String startChar) {
                this.startChar = startChar;
            }

            public String getLineEndingChar() {
                return lineEndingChar;
            }

            public void setLineEndingChar(String lineEndingChar) {
                this.lineEndingChar = lineEndingChar;
            }

            public String getEndChar() {
                return endChar;
            }

            public void setEndChar(String endChar) {
                this.endChar = endChar;
            }
        }
    }


    public class FixedWidth {
        // Fixed Length Files
        private int rowSize; // total size of a row.
        private HashMap<Integer, Integer> colIndexSize; // {(0, 20), (1,31)...(5,25)}
        private BoundaryDefinition boundaryDefinition;

        FixedWidth() {
            this.boundaryDefinition = new FixedWidth.BoundaryDefinition();
        }

        public int getRowSize() {
            return rowSize;
        }

        public void setRowSize(int rowSize) {
            this.rowSize = rowSize;
        }

        public HashMap<Integer, Integer> getColIndexSize() {
            return colIndexSize;
        }

        public void setColIndexSize(HashMap<Integer, Integer> colIndexSize) {
            this.colIndexSize = colIndexSize;
        }

        public FixedWidth.BoundaryDefinition getBoundaryDefinition() {
            return boundaryDefinition;
        }

        public void setBoundaryDefinition(FixedWidth.BoundaryDefinition boundaryDefinition) {
            this.boundaryDefinition = boundaryDefinition;
        }

        public class BoundaryDefinition {
            private String rowPatternStart;
            private String rowPatterEnd;


            public String getRowPatternStart() {
                return rowPatternStart;
            }

            public void setRowPatternStart(String rowPatternStart) {
                this.rowPatternStart = rowPatternStart;
            }

            public String getRowPatterEnd() {
                return rowPatterEnd;
            }

            public void setRowPatterEnd(String rowPatterEnd) {
                this.rowPatterEnd = rowPatterEnd;
            }
        }


    }




}


