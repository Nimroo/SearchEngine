package ir.sahab.nimroo.serialization;

import ir.sahab.nimroo.model.Link;
import ir.sahab.nimroo.model.Meta;
import ir.sahab.nimroo.model.PageData;

import java.util.ArrayList;
import java.util.Optional;

/**
 * @author ArminF96
 */
public class PageDataSerializer {

  private static PageDataSerializer ourInstance = new PageDataSerializer();

  public static PageDataSerializer getInstance() {
    return  new PageDataSerializer();//ourInstance;
  }

  private PageDataSerializer() {}

  public byte[] serialize(PageData pageData) {

    PageDataProto.PageData.Builder pageBuilder = PageDataProto.PageData.newBuilder();
    Optional.ofNullable(pageData.getUrl()).ifPresent(pageBuilder::setUrl);
    Optional.ofNullable(pageData.getTitle()).ifPresent(pageBuilder::setTitle);
    Optional.ofNullable(pageData.getText()).ifPresent(pageBuilder::setText);

    PageDataProto.Link.Builder linkBuilder = PageDataProto.Link.newBuilder();
    for (Link link : pageData.getLinks()) {
      linkBuilder.clear();
      Optional.ofNullable(link.getLink()).ifPresent(linkBuilder::setLink);
      Optional.ofNullable(link.getAnchor()).ifPresent(linkBuilder::setAnchor);
      pageBuilder.addLinks(linkBuilder.build());
    }

    PageDataProto.Meta.Builder metaBuilder = PageDataProto.Meta.newBuilder();
    for (Meta meta : pageData.getMetas()) {
      metaBuilder.clear();
      Optional.ofNullable(meta.getName()).ifPresent(metaBuilder::setName);
      Optional.ofNullable(meta.getContent()).ifPresent(metaBuilder::setContent);
      Optional.ofNullable(meta.getCharset()).ifPresent(metaBuilder::setCharset);
      Optional.ofNullable(meta.getHttpEquiv()).ifPresent(metaBuilder::setHttpEquiv);
      Optional.ofNullable(meta.getScheme()).ifPresent(metaBuilder::setScheme);
      pageBuilder.addMetas(metaBuilder.build());
    }
    Optional.ofNullable(pageData.getH1()).ifPresent(pageBuilder::setH1);
    Optional.ofNullable(pageData.getH2()).ifPresent(pageBuilder::addAllH2);
    PageDataProto.PageData protoPageData = pageBuilder.build();
    return protoPageData.toByteArray();
  }

  public PageData deserialize(byte[] input) throws com.github.os72.protobuf351.InvalidProtocolBufferException {
    PageData pageData = new PageData();
    PageDataProto.PageData protoPageData = PageDataProto.PageData.parseFrom(input);
    pageData.setUrl(protoPageData.getUrl());
    pageData.setTitle(protoPageData.getTitle());
    pageData.setText(protoPageData.getText());
    pageData.setH1(protoPageData.getH1());
    pageData.setH2(new ArrayList<>(protoPageData.getH2List()));
    ArrayList<Link> links = new ArrayList<>();
    for (PageDataProto.Link protoLink : protoPageData.getLinksList()) {
      Link link = new Link();
      link.setAnchor(protoLink.getAnchor());
      link.setLink(protoLink.getLink());
      links.add(link);
    }
    pageData.setLinks(links);

    ArrayList<Meta> metas = new ArrayList<>();
    for (PageDataProto.Meta protoMeta : protoPageData.getMetasList()) {
      Meta meta = new Meta();
      meta.setName(protoMeta.getName());
      meta.setScheme(protoMeta.getScheme());
      meta.setCharset(protoMeta.getCharset());
      meta.setContent(protoMeta.getContent());
      meta.setHttpEquiv(protoMeta.getHttpEquiv());
      metas.add(meta);
    }
    pageData.setMetas(metas);

    return pageData;
  }
}
