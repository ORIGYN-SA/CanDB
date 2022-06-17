
import Text "mo:base/Text";
import CanDB "../../../src/CanDBv2";
import Principal "mo:base/Principal";


// Simple Blogging Application 

import Entity "../../../src/Entity";
import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Int "mo:base/Int";
import Option "mo:base/Option";
import Debug "mo:base/Debug";

/*
/// Note: This is example is not meant to be used as is in a system handling assets of value. It does not include 
/// access control or any security measures, and is purely meant to showcase example usage of CanDB

/// This example shows how one might support a application that keeps track of balances and transactions with CanDB
*/

/// First, define access patterns for the application
///
/// access patterns
/// 1. get a specific blog post by a user
/// 2. get latest blog posts for a user
/// 3. get latest comments for a blog post
/// 4. get user settings
/// 5. create a blog post
/// 6. create comments for a blog post (user has to approve)
/// 
/// 2 different pk/sk combinations needed
/// 
/// PK                   SK                              Meets Access Patern(s)
/// 
/// user#[userId]        transaction#[transactionId]     (1, 2, 4)
/// transaction          transaction#[transactionId]     (3)
///
/// Then determine APIs needed to support those access patterns
///
/// - getCurrentUserBalance (access pattern 1)
/// - getUserBalanceHistory (access pattern 2)
/// - getLatestTransactions (access pattern 3)
/// - getLatestUserTransactions (access pattern 4)
/// - createUserDeposit
/// - createTransaction

/* TODO: use this again once can blob encode records
shared ({ caller = owner }) actor class UserCanister({ 
  pk: Text; 
  owners: ?[Principal];
}) {
*/

shared ({ caller = owner }) actor class UserCanister(pk: Text) {
  // Initialize CanDB
  // stable let owners = owners; // TODO: put this back in once can blob encode records
  stable let db = CanDB.init({
    pk = pk;
  });

  let blogPostSKPrefix = "post#";

  // Time package does not work locally, so use incrementing transactionIdCounter as a unique 
  // identifier instead of using UUID/ULID (requires the Time package)
  stable var postIdCounter = 1;
  stable var commentIdCounter = 1;

  // APIs

  public type GetBlogPostRequest = {
    postId: Int;
  };

  public type GetBlogPostResponse = BlogPostEntity; 

  public query func getBlogPostById(request: GetBlogPostRequest): async ?GetBlogPostResponse {
    getBlogPostByPostId(request.postId);
  };

  public type GetBlogPostsByTitleRequest = {
    title: Text;
  };

  public type GetBlogPostsByTitleResponse = ScanBlogPostsResponse; 

  public query func getBlogPostsByTitle(request: GetBlogPostsByTitleRequest): async GetBlogPostsByTitleResponse {
    getBlogPostsByPostTitle(request.title);
  };

  type GetBlogPostCommentsRequest = {
    postId: Int;
    limit: Nat;
    nextKey: ?Text;
  };

  type GetBlogPostCommentsResponse = {
    comments: [BlogPostCommentEntity];
    nextKey: ?Text;
  };

  public query func getBlogPostComments(request: GetBlogPostCommentsRequest): async GetBlogPostCommentsResponse {
    let upperBound = Option.get(request.nextKey, "post#" # Int.toText(request.postId) # "_comment#:");
    let { entities; nextKey }= CanDB.scan(db, {
      skLowerBound = "post#" # Int.toText(request.postId) # "_comment#";
      skUpperBound = upperBound;
      limit = request.limit;
      ascending = ?false;
    });
    
    {
      comments = Array.mapFilter(entities, unwrapBlogPostCommentEntity);
      nextKey = nextKey;
    }
  };

  public type CreateBlogPostRequest = {
    title: Text;
    body: Text;
  };

  public type CreateBlogPostResponse = {
    postId: Nat;
  };

  public shared ({caller = caller}) func createBlogPost(request: CreateBlogPostRequest): async ?CreateBlogPostResponse {
    if (not callerIsUser(caller)) { return null };

    let postId = postIdCounter;

    // insert a blog post by its postId 
    CanDB.put(db, {
      sk = "post#" # Int.toText(postId); 
      attributes = [
        ("title", #text(request.title)),
        ("body", #text(request.body)),
        ("postId", #int(postId)),
      ]
    });

    // insert a blog post by its title and postId 
    CanDB.put(db, {
      sk = "title#" # request.title # "_post#" # Int.toText(postId);
      attributes = [
        ("title", #text(request.title)),
        ("postId", #int(postId)),
      ];
    });

    postIdCounter += 1;

    ?{
      postId = postId;
    };
  };

  public type CreateBlogPostCommentRequest = {
    postId: Int;
    commentBody: Text;
  };

  public type CreateBlogPostCommentResponse = {
    commentId: Nat;
  };

  public shared ({caller = caller}) func createBlogPostComment(request: CreateBlogPostCommentRequest): async ?CreateBlogPostCommentResponse {
    if (Option.isNull(getBlogPostByPostId(request.postId))) { return null };

    let commentId = commentIdCounter;
    CanDB.put(db, {
      sk = "post#" # Int.toText(request.postId) # "_comment#" # Int.toText(commentId);
      attributes = [
        ("postId", #int(request.postId)),
        ("commentId", #int(commentId)),
        ("commentBody", #text(request.commentBody)),
      ]
    });

    commentIdCounter += 1;

    ?{
      commentId = commentId;
    }
  };
  

  func callerIsUser(callerPrincipal: Principal): Bool {
    db.pk == "user#" # Principal.toText(callerPrincipal);
  };

  func getBlogPostByPostId(postId: Int): ?GetBlogPostResponse {
    switch(CanDB.get(db, { sk = "post#" # Int.toText(postId); })) {
      case null { null };
      case (?postEntity) { unwrapBlogPostEntity(postEntity) }
    }
  };


  public type ScanBlogPostsResponse = {
    posts: [BlogPostEntity];
    nextKey: ?Entity.SK;
  };

  func getBlogPostsByPostTitle(title: Text): ScanBlogPostsResponse {
    let { entities; nextKey } = CanDB.scan(db, {
      skLowerBound = "title#" # title # "_"; // match on full title and not prefix
      skUpperBound = "title#" # title # "_q"; // q comes after p in post
      limit = 10;
      ascending = ?true;
    });

    let posts = Array.mapFilter<Entity.Entity, BlogPostEntity>(entities, func(e) {
      switch(Entity.getAttributeMapValueForKey(e.attributes, "postId")) {
        case null { null };
        case (?(#int(postId))) { 
          switch(CanDB.get(db, { sk = "post#" # Int.toText(postId); })) {
            case null { null };
            case (?postEntity) { unwrapBlogPostEntity(postEntity) };
          }
        };
        case _ {
          Debug.print("Error in getBlogPostsByPostTitle: improperly formatted blog post");
          null
        }
      } 
    });
    
    {
      posts = posts;
      nextKey = nextKey;
    }
  };

  type BlogPostEntity = {
    postId: Int;
    title: Text;
    body: Text;
  };

  func unwrapBlogPostEntity(entity: Entity.Entity): ?BlogPostEntity {
    let titleValue = Entity.getAttributeMapValueForKey(entity.attributes, "title");
    let bodyValue = Entity.getAttributeMapValueForKey(entity.attributes, "body");
    let postIdValue = Entity.getAttributeMapValueForKey(entity.attributes, "postId");
    switch(titleValue, bodyValue, postIdValue) {
      case (
        ?(#text(title)),
        ?(#text(body)),
        ?(#int(postId)),
      ) {
        ? {
          postId = postId;
          title = title;
          body = body;
        }
      };
      case _ {
        Debug.print("error attempting to parse/unwrap improperly formatted blog post");
        null
      };
    }
  };

  type BlogPostCommentEntity = {
    postId: Int;
    commentId: Int;
    commentBody: Text;
  };

  func unwrapBlogPostCommentEntity(entity: Entity.Entity): ?BlogPostCommentEntity {
    let postIdValue = Entity.getAttributeMapValueForKey(entity.attributes, "postId");
    let commentIdValue = Entity.getAttributeMapValueForKey(entity.attributes, "commentId");
    let commentBodyValue = Entity.getAttributeMapValueForKey(entity.attributes, "commentBody");
    switch(postIdValue, commentIdValue, commentBodyValue) {
      case (
        ?(#int(postId)),
        ?(#int(commentId)),
        ?(#text(commentBody)),
      ) {
        ? {
          postId = postId;
          commentId = commentId;
          commentBody = commentBody;
        }
      };
      case _ {
        Debug.print("error attempting to parse/unwrap improperly formatted blog post comment");
        null
      };
    }

  }
}