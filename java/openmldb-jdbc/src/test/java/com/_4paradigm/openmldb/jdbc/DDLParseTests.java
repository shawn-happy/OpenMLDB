package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.*;
import com._4paradigm.openmldb.sdk.DataType;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.TTLType;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Ignore
public class DDLParseTests {

  @Test
  public void genDDLNormal() throws Exception {
    String sql = "SELECT\n"
            + "  a.itemId as itemId,\n"
            + "  a.ip as ip,\n"
            + "  a.query as query,\n"
            + "  a.mcuid as mcuid,\n"
            + "  c.brandName as name,\n"
            + "  c.brandId as brandId,\n"
            + "  b.actionValue as label\n"
            + "FROM a\n"
            + "  LAST JOIN b ON b.itemId = a.itemId\n"
            + "  LAST JOIN c ON a.itemId = c.id;";
    Map<String, List<Map<String, String>>> tableMap = new HashMap<>();

    List<Map<String, String>> aColumnMapList = new ArrayList<>();

    Map<String, String> aItemColumnMap = new HashMap<>();
    aItemColumnMap.put("name", "itemId");
    aItemColumnMap.put("type", DataType.kString.name());

    Map<String, String> aIpColumnMap = new HashMap<>();
    aIpColumnMap.put("name", "ip");
    aIpColumnMap.put("type", DataType.kString.name());

    Map<String, String> aQueryColumnMap = new HashMap<>();
    aQueryColumnMap.put("name", "query");
    aQueryColumnMap.put("type", DataType.kString.name());

    Map<String, String> aMcuidColumnMap = new HashMap<>();
    aMcuidColumnMap.put("name", "mcuid");
    aMcuidColumnMap.put("type", DataType.kString.name());

    aColumnMapList.add(aItemColumnMap);
    aColumnMapList.add(aIpColumnMap);
    aColumnMapList.add(aQueryColumnMap);
    aColumnMapList.add(aMcuidColumnMap);

    List<Map<String, String>> bColumnMapList = new ArrayList<>();
    Map<String, String> bActionValue = new HashMap<>();
    bActionValue.put("name", "actionValue");
    bActionValue.put("type", DataType.kInt.name());

    Map<String, String> bItem = new HashMap<>();
    bItem.put("name", "itemId");
    bItem.put("type", DataType.kString.name());

    bColumnMapList.add(bActionValue);
    bColumnMapList.add(bItem);

    List<Map<String, String>> cColumnMapList = new ArrayList<>();
    Map<String, String> cId = new HashMap<>();
    cId.put("name", "id");
    cId.put("type", DataType.kString.name());

    Map<String, String> cBrandName = new HashMap<>();
    cBrandName.put("name", "brandName");
    cBrandName.put("type", DataType.kString.name());

    Map<String, String> cBrandId = new HashMap<>();
    cBrandId.put("name", "brandId");
    cBrandId.put("type", DataType.kString.name());

    cColumnMapList.add(cId);
    cColumnMapList.add(cBrandName);
    cColumnMapList.add(cBrandId);

    tableMap.put("a", aColumnMapList);
    tableMap.put("b", bColumnMapList);
    tableMap.put("c", cColumnMapList);

    SqlExecutor sqlExecutor = Mockito.mock(SqlClusterExecutor.class);
    SimpleMapVectorMap simpleMapVectorMap = Mockito.mock(SimpleMapVectorMap.class);
    SimpleMapVector aColumnKeyVector = Mockito.mock(SimpleMapVector.class);

    SimpleMap aItemId = Mockito.mock(SimpleMap.class);
    Mockito.when(aItemId.get("col_list")).thenReturn("itemId");
    Mockito.when(aItemId.get("ttl_type")).thenReturn(TTLType.kAbsAndLat.name());
    Mockito.when(aItemId.get("abs_ttl")).thenReturn("60");
    Mockito.when(aColumnKeyVector.get(0)).thenReturn(aItemId);

    SimpleMapVector bColumnKeyVector = Mockito.mock(SimpleMapVector.class);
    SimpleMap bItemId = Mockito.mock(SimpleMap.class);
    Mockito.when(bItemId.get("col_list")).thenReturn("itemId");
    Mockito.when(bItemId.get("ttl_type")).thenReturn(TTLType.kAbsAndLat.name());
    Mockito.when(bItemId.get("abs_ttl")).thenReturn("60");
    Mockito.when(bColumnKeyVector.get(0)).thenReturn(bItemId);

    SimpleMapVector cColumnKeyVector = Mockito.mock(SimpleMapVector.class);
    SimpleMap cItemId = Mockito.mock(SimpleMap.class);
    Mockito.when(cItemId.get("col_list")).thenReturn("itemId");
    Mockito.when(cItemId.get("ttl_type")).thenReturn(TTLType.kAbsAndLat.name());
    Mockito.when(cItemId.get("abs_ttl")).thenReturn("60");
    Mockito.when(cColumnKeyVector.get(0)).thenReturn(cItemId);

    Mockito.when(simpleMapVectorMap.get(("a"))).thenReturn(aColumnKeyVector);
    Mockito.when(simpleMapVectorMap.get(("b"))).thenReturn(bColumnKeyVector);
    Mockito.when(simpleMapVectorMap.get(("c"))).thenReturn(cColumnKeyVector);
    Mockito.when(sqlExecutor.getColumnKey(sql, tableMap)).thenReturn(simpleMapVectorMap);
    SimpleMapVectorMap columnKey = sqlExecutor.getColumnKey(sql, tableMap);
    Assert.assertFalse(columnKey.isEmpty());
    Assert.assertEquals("itemId", columnKey.get("a").get(0).get("col_list"));
    Assert.assertEquals("itemId", columnKey.get("b").get(0).get("col_list"));
    Assert.assertEquals("itemId", columnKey.get("c").get(0).get("col_list"));
  }

}
