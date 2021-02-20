/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.elastic.demo.stocks.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SymbolDateKey implements Writable, WritableComparable<SymbolDateKey>
{
    private final Text symbol = new Text();
    private final Text date   = new Text();
    private final Text docid  = new Text();

    public SymbolDateKey() { }

    public Text symbol()
    {
        return symbol;
    }

    public Text docid()
    {
        return docid;
    }

    public SymbolDateKey(String symbol, String date, String docid)
    {
        this.symbol.set(symbol);
        this.date.set(date);
        this.docid.set(docid);
    }

    @Override
    public int compareTo(SymbolDateKey sdk)
    {
        int compare = this.symbol.compareTo(sdk.symbol);
        if (compare == 0) {
            compare = this.date.compareTo(sdk.date);
        }
        return compare;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        symbol.write(out);
        date.write(out);
        docid.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        symbol.readFields(in);
        date.readFields(in);
        docid.readFields(in);
    }

    @Override
    public String toString()
    {
        return symbol.toString() + "." + date.toString();
    }
}
