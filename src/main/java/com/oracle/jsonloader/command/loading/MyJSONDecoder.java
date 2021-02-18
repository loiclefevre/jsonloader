package com.oracle.jsonloader.command.loading;

import com.oracle.jsonloader.util.MyOracleJsonFactory;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import oracle.sql.json.OracleJsonParser;
import org.bson.BSONCallback;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import javax.json.stream.JsonGenerator;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class MyJSONDecoder {
    protected final boolean outputOsonFormat;

    protected final MyOracleJsonFactory factory = new MyOracleJsonFactory();
    protected final ByteArrayOutputStream out = new ByteArrayOutputStream();
    protected int stringLength;

    public static void main(String[] args) {
        MyJSONDecoder d = new MyJSONDecoder(true);

        d.readObject("{\"id\":\"2507079547\",\"type\":\"ReleaseEvent\",\"actor\":{\"id\":10105136,\"login\":\"ChristianPSchenk\",\"gravatar_id\":\"\",\"url\":\"https://api.github.com/users/ChristianPSchenk\",\"avatar_url\":\"https://avatars.githubusercontent.com/u/10105136?\"},\"repo\":{\"id\":27670575,\"name\":\"ChristianPSchenk/JRTrace\",\"url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace\"},\"payload\":{\"action\":\"published\",\"release\":{\"url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/releases/844202\",\"assets_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/releases/844202/assets\",\"upload_url\":\"https://uploads.github.com/repos/ChristianPSchenk/JRTrace/releases/844202/assets{?name}\",\"html_url\":\"https://github.com/ChristianPSchenk/JRTrace/releases/tag/v0.3.1\",\"id\":844202,\"tag_name\":\"v0.3.1\",\"target_commitish\":\"master\",\"name\":\"v0.3.1 development release\",\"draft\":false,\"author\":{\"login\":\"ChristianPSchenk\",\"id\":10105136,\"avatar_url\":\"https://avatars.githubusercontent.com/u/10105136?v=3\",\"gravatar_id\":\"\",\"url\":\"https://api.github.com/users/ChristianPSchenk\",\"html_url\":\"https://github.com/ChristianPSchenk\",\"followers_url\":\"https://api.github.com/users/ChristianPSchenk/followers\",\"following_url\":\"https://api.github.com/users/ChristianPSchenk/following{/other_user}\",\"gists_url\":\"https://api.github.com/users/ChristianPSchenk/gists{/gist_id}\",\"starred_url\":\"https://api.github.com/users/ChristianPSchenk/starred{/owner}{/repo}\",\"subscriptions_url\":\"https://api.github.com/users/ChristianPSchenk/subscriptions\",\"organizations_url\":\"https://api.github.com/users/ChristianPSchenk/orgs\",\"repos_url\":\"https://api.github.com/users/ChristianPSchenk/repos\",\"events_url\":\"https://api.github.com/users/ChristianPSchenk/events{/privacy}\",\"received_events_url\":\"https://api.github.com/users/ChristianPSchenk/received_events\",\"type\":\"User\",\"site_admin\":false},\"prerelease\":true,\"created_at\":\"2015-01-12T15:58:39Z\",\"published_at\":\"2015-01-12T16:00:00Z\",\"assets\":[],\"tarball_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/tarball/v0.3.1\",\"zipball_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/zipball/v0.3.1\",\"body\":\"<ul><li>Problems with the JRTrace code will be reported in the Problems Log\\r\\n</li><li>Improved process selection dialog</li>\\r\\n<li>Several new options for trace code insertion: exclude of packages, instrumentation of method invocations and field access</li></ul>\"}},\"public\":true,\"created_at\":\"2015-01-12T16:00:00Z\"}");
        d.getOSONData();
        d.readObject("{\"id\":\"2507079547\",\"type\":\"ReleaseEvent\",\"actor\":{\"id\":10105136,\"login\":\"ChristianPSchenk\",\"gravatar_id\":\"\",\"url\":\"https://api.github.com/users/ChristianPSchenk\",\"avatar_url\":\"https://avatars.githubusercontent.com/u/10105136?\"},\"repo\":{\"id\":27670575,\"name\":\"ChristianPSchenk/JRTrace\",\"url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace\"},\"payload\":{\"action\":\"published\",\"release\":{\"url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/releases/844202\",\"assets_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/releases/844202/assets\",\"upload_url\":\"https://uploads.github.com/repos/ChristianPSchenk/JRTrace/releases/844202/assets{?name}\",\"html_url\":\"https://github.com/ChristianPSchenk/JRTrace/releases/tag/v0.3.1\",\"id\":844202,\"tag_name\":\"v0.3.1\",\"target_commitish\":\"master\",\"name\":\"v0.3.1 development release\",\"draft\":false,\"author\":{\"login\":\"ChristianPSchenk\",\"id\":10105136,\"avatar_url\":\"https://avatars.githubusercontent.com/u/10105136?v=3\",\"gravatar_id\":\"\",\"url\":\"https://api.github.com/users/ChristianPSchenk\",\"html_url\":\"https://github.com/ChristianPSchenk\",\"followers_url\":\"https://api.github.com/users/ChristianPSchenk/followers\",\"following_url\":\"https://api.github.com/users/ChristianPSchenk/following{/other_user}\",\"gists_url\":\"https://api.github.com/users/ChristianPSchenk/gists{/gist_id}\",\"starred_url\":\"https://api.github.com/users/ChristianPSchenk/starred{/owner}{/repo}\",\"subscriptions_url\":\"https://api.github.com/users/ChristianPSchenk/subscriptions\",\"organizations_url\":\"https://api.github.com/users/ChristianPSchenk/orgs\",\"repos_url\":\"https://api.github.com/users/ChristianPSchenk/repos\",\"events_url\":\"https://api.github.com/users/ChristianPSchenk/events{/privacy}\",\"received_events_url\":\"https://api.github.com/users/ChristianPSchenk/received_events\",\"type\":\"User\",\"site_admin\":false},\"prerelease\":true,\"created_at\":\"2015-01-12T15:58:39Z\",\"published_at\":\"2015-01-12T16:00:00Z\",\"assets\":[],\"tarball_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/tarball/v0.3.1\",\"zipball_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/zipball/v0.3.1\",\"body\":\"<ul><li>Problems with the JRTrace code will be reported in the Problems Log\\r\\n</li><li>Improved process selection dialog</li>\\r\\n<li>Several new options for trace code insertion: exclude of packages, instrumentation of method invocations and field access</li></ul>\"}},\"public\":true,\"created_at\":\"2015-01-12T16:00:00Z\"}");
        d.getOSONData();
        d.readObject("{\"id\":\"2507079547\",\"type\":\"ReleaseEvent\",\"actor\":{\"id\":10105136,\"login\":\"ChristianPSchenk\",\"gravatar_id\":\"\",\"url\":\"https://api.github.com/users/ChristianPSchenk\",\"avatar_url\":\"https://avatars.githubusercontent.com/u/10105136?\"},\"repo\":{\"id\":27670575,\"name\":\"ChristianPSchenk/JRTrace\",\"url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace\"},\"payload\":{\"action\":\"published\",\"release\":{\"url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/releases/844202\",\"assets_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/releases/844202/assets\",\"upload_url\":\"https://uploads.github.com/repos/ChristianPSchenk/JRTrace/releases/844202/assets{?name}\",\"html_url\":\"https://github.com/ChristianPSchenk/JRTrace/releases/tag/v0.3.1\",\"id\":844202,\"tag_name\":\"v0.3.1\",\"target_commitish\":\"master\",\"name\":\"v0.3.1 development release\",\"draft\":false,\"author\":{\"login\":\"ChristianPSchenk\",\"id\":10105136,\"avatar_url\":\"https://avatars.githubusercontent.com/u/10105136?v=3\",\"gravatar_id\":\"\",\"url\":\"https://api.github.com/users/ChristianPSchenk\",\"html_url\":\"https://github.com/ChristianPSchenk\",\"followers_url\":\"https://api.github.com/users/ChristianPSchenk/followers\",\"following_url\":\"https://api.github.com/users/ChristianPSchenk/following{/other_user}\",\"gists_url\":\"https://api.github.com/users/ChristianPSchenk/gists{/gist_id}\",\"starred_url\":\"https://api.github.com/users/ChristianPSchenk/starred{/owner}{/repo}\",\"subscriptions_url\":\"https://api.github.com/users/ChristianPSchenk/subscriptions\",\"organizations_url\":\"https://api.github.com/users/ChristianPSchenk/orgs\",\"repos_url\":\"https://api.github.com/users/ChristianPSchenk/repos\",\"events_url\":\"https://api.github.com/users/ChristianPSchenk/events{/privacy}\",\"received_events_url\":\"https://api.github.com/users/ChristianPSchenk/received_events\",\"type\":\"User\",\"site_admin\":false},\"prerelease\":true,\"created_at\":\"2015-01-12T15:58:39Z\",\"published_at\":\"2015-01-12T16:00:00Z\",\"assets\":[],\"tarball_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/tarball/v0.3.1\",\"zipball_url\":\"https://api.github.com/repos/ChristianPSchenk/JRTrace/zipball/v0.3.1\",\"body\":\"<ul><li>Problems with the JRTrace code will be reported in the Problems Log\\r\\n</li><li>Improved process selection dialog</li>\\r\\n<li>Several new options for trace code insertion: exclude of packages, instrumentation of method invocations and field access</li></ul>\"}},\"public\":true,\"created_at\":\"2015-01-12T16:00:00Z\"}");
        byte[] bytes = d.getOSONData();
        System.out.println(d.getLength()+" -> "+bytes.length);

    }

    public MyJSONDecoder(boolean outputOsonFormat) {
        super();
        this.outputOsonFormat = outputOsonFormat;
    }

    public void readObject(String bytes) {
        out.reset();
        stringLength = bytes.getBytes().length;
        OracleJsonParser parser = factory.createJsonTextParser(new StringReader(bytes));
        OracleJsonGenerator ogen = outputOsonFormat ? factory.createJsonBinaryGenerator(out) : factory.createJsonTextGenerator(out);
        //JsonGenerator gen = ogen.wrap(JsonGenerator.class);
        ogen.writeParser(parser);
        ogen.close();
    }

    public byte[] getOSONData() {
        return out.toByteArray();
    }

    public int getLength() {
        return stringLength;
    }

}
