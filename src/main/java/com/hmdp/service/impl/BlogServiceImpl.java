package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;
import static java.lang.System.currentTimeMillis;

/**
 *
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    IUserService userService;
    @Resource
    IFollowService followService;

    /**
     * 点赞功能实现：
     * 点赞一篇笔记，实现一人一赞
     * 思路：
     *    利用redis记录： key（笔记id）：value（每个用户id）
     *     因为是一人一赞，所以采用SET结构
     *     然后修改数据库
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {

        //是否点过赞
        //没有：点赞   有：取消点赞

        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        String key=BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //log.debug("score："+score+",userId:"+userId);
       if(score==null) {
           boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
           if (isSuccess) {

               stringRedisTemplate.opsForZSet()
                       .add(key, userId.toString(),System.currentTimeMillis());
             //System.currentTimeMillis()
           }
       }else {
           boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
           // 4.2.把用户从Redis的set集合移除
           if (isSuccess) {
               stringRedisTemplate.opsForZSet().remove(key, userId.toString());
           }
       }

        return Result.ok();
    }

    @Override
    public Result queryHotBlog(Integer current) {
                // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
       queryBlogUser(blog);
       isBlogLiked(blog);

        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
//        // 查询详情
//        User user = userService.getById(id);
//        if (user == null) {
//            return Result.ok();
//        }
//        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
//        // 返回
//        return Result.ok(userDTO);
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        queryBlogUser(blog);
        isBlogLiked(blog);
        return  Result.ok(blog);
    }


    public void isBlogLiked(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        String key = "blog:liked:" + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogLikes(Long id) {

        String key=BLOG_LIKED_KEY+id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
            if(top5==null||top5.isEmpty()) return Result.ok(Collections.emptyList());

        List<Long> list = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", list);
        //根据用户id查询用户 WHERE id IN ( 5 , 1 ) ORDER BY FIELD(id, 5, 1)
        List<User> userList = userService.query().in("id", list)
                .last("ORDER BY FIELD ( id ," + idStr + ")").list();
        List<UserDTO> userDTOList = userList.stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(userDTOList);
    }


    /**
     * 在存储发布的笔记
     * 采用Feed流的推模式将发布的消息传给粉丝
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess) return  Result.fail("新增笔记失败");
        //推送探店笔记给用户粉丝
        List<Follow> follows = followService.query()
                .eq("follow_user_id", user.getId()).list();
//        List<Long> fensiList = follows.stream().map(Follow::getUserId).collect(Collectors.toList());
            for(Follow follow: follows){
                Long userId = follow.getUserId();
                String key=FEED_KEY+userId;
                stringRedisTemplate.opsForZSet()
                        .add(key,blog.getId().toString()
                        ,System.currentTimeMillis());
            }

        // 返回id
        return Result.ok(blog.getId());
    }

    /**
     * 滚动分页查询
     * redis语句：  reverseRangeByScore： key  max  min   offset  number
     *
     * 其中min始终为0，需要max和偏移量，这个接口已经请求自带
     * 为了下次的滚动分页查询，需要返回这次最小的score（最后一次的score）作为下次的max ，
     * 然后最后的value重复了几次，作为offset的大小
     * @param max
     * @param offset
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {

        Long userId = UserHolder.getUser().getId();
        String key=FEED_KEY+userId;
        // 2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0,
                        max, offset, 2);
        if(typedTuples==null||typedTuples.isEmpty())
            return Result.ok();


    // 4.解析数据：blogId、minTime（时间戳）、offset
    List<Long> ids = new ArrayList<>(typedTuples.size());
    long minTime = 0; // 2
    int os = 1; // 2
        for(ZSetOperations.TypedTuple<String>typedTuple:typedTuples){
            ids.add(Long.valueOf(typedTuple.getValue()));
           long time = typedTuple.getScore().longValue();
            if(minTime==time){
                os++;
            }else {
                minTime=time;
                os=1;
            }
        }

        // 5.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
            // 5.1.查询blog有关的用户
            queryBlogUser(blog);
            // 5.2.查询blog是否被点赞
            isBlogLiked(blog);
        }

        // 6.封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);

        return Result.ok(r);
    }
}

